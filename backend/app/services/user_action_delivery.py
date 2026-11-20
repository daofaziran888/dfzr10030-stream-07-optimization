"""用户动作交付：规则命中后的动作去重与真实投递。

规则引擎把「哪位用户 / 哪个业务事件 / 哪类动作」的命中交给事件路由。事件路由
不再按规则各写一份动作，而是先落到用户动作归属清单：

* 归属键 = (用户ID, 业务事件, 动作类别)。同一个归属键只允许存在一份有效动作，
  不同规则命中同一个人时把规则编号追加到「命中来源」，其他用户照常处理。
* 归属确认后写入用户动作交付批次，批次里的投递状态区分「已确认」和「待重试」；
  只有真正写进目标管道才标记「已交付」并生成交付回执。
* 接收管道凭交付回执可以反查订单；已经由一条管道接单的订单，另一条管道即使
  收到原样补送也不能再领到同一笔工作。
* 目标管道缺少该动作类别的交付权限时直接判为无法投递，不会改用其他管道。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Callable, Iterable

from app.store import store

MODULE = "user_action_delivery"
OWNERSHIP_MODULE = "user_action_ownership"
BATCH_MODULE = "user_action_delivery_batch"
INBOX_MODULE = "_pipeline_inbox"

# 归属 / 投递状态
STATUS_PENDING = "待投递"
STATUS_CONFIRMED = "已确认"
STATUS_RETRY = "待重试"
STATUS_DELIVERED = "已交付"
STATUS_REJECTED = "无法投递"

ACTIVE_STATUSES = {STATUS_PENDING, STATUS_CONFIRMED, STATUS_RETRY, STATUS_DELIVERED}
DELIVERABLE_STATUSES = {STATUS_CONFIRMED, STATUS_RETRY}

# 动作类别 -> 允许接收的管道。未登记的动作类别不做额外限制；显式携带
# 「交付权限」/「可投递」为 False 或允许管道列表时以显式配置为准。
DELIVERY_PERMISSIONS: dict[str, set[str]] = {
    "用户标记": {"pipeline-order-process", "pipeline-user-profile"},
    "用户打标": {"pipeline-order-process", "pipeline-user-profile"},
    "订单标记": {"pipeline-order-process", "pipeline-payment-audit"},
    "支付标记": {"pipeline-payment-audit"},
    "地区标记": {"pipeline-user-profile", "pipeline-order-process"},
    "地区迁移": {"pipeline-user-profile", "pipeline-order-process"},
    "异常订单": {"pipeline-order-process", "pipeline-payment-audit"},
    "高频用户": {"pipeline-user-profile", "pipeline-order-process"},
    "高价值用户": {"pipeline-user-profile", "pipeline-order-process"},
}

DEDUP_KEY_FIELDS = ("用户ID", "业务事件", "动作类别")

# 并发提交时保证「查重 -> 写入归属 -> 写交付批次」是原子的。
_LOCK = threading.RLock()


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------
def _now(value: str | None = None) -> str:
    return value or datetime.now().isoformat()


def _pick(mapping: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping:
            value = mapping[key]
            if value is not None and value != "":
                return value
    return default


def _as_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _as_priority(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _norm_hit(hit: dict[str, Any]) -> dict[str, Any]:
    """把中英文命名字段统一成内部命中结构，外部调用方不用关心字段别名。"""
    order = _pick(hit, "订单编号", "order_id", "订单")
    event = _pick(
        hit, "业务事件", "业务事件编号", "business_event", "event_id", "event"
    )
    event = _as_text(event) or _as_text(order)
    category = _pick(hit, "动作类别", "action_category", "category")
    action = _pick(hit, "动作", "action")
    category = _as_text(category) or _as_text(action)
    target = _pick(
        hit, "目标管道", "接收管道", "target_pipeline", "pipeline", "管道"
    )
    permission = _pick(
        hit, "交付权限", "delivery_permission", "可投递", "allowed", default=None
    )
    return {
        "用户ID": _as_text(_pick(hit, "用户ID", "用户", "user_id", "user")),
        "业务事件": event,
        "动作类别": category,
        "订单编号": _as_text(order) or event,
        "动作": _as_text(action) or category,
        "规则编号": _as_text(_pick(hit, "规则编号", "rule_id", "rule", "命中规则")),
        "目标管道": _as_text(target),
        "优先级": _as_priority(_pick(hit, "优先级", "priority", default=0)),
        "交付权限": permission,
        "触发时间": _as_text(
            _pick(hit, "触发时间", "timestamp", "event_time", "triggered_at")
        ),
    }


def _missing_fields(hit: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    if not hit["用户ID"]:
        missing.append("用户ID")
    if not hit["业务事件"]:
        missing.append("业务事件")
    if not hit["动作类别"]:
        missing.append("动作类别")
    return missing


def _permission_error(hit: dict[str, Any]) -> str | None:
    """返回交付权限错误；没有任何「换一个目标管道」的兜底。"""
    target = _as_text(hit.get("目标管道"))
    if not target or target.casefold() == "default":
        return "缺少目标管道，动作不得改投其他管道"
    permission = hit.get("交付权限")
    if permission is False:
        return f"动作「{hit.get('动作类别', '')}」缺少交付权限，不得改投其他管道"
    if isinstance(permission, str) and permission.strip():
        permission = [permission]
    if isinstance(permission, (list, tuple, set)):
        allowed = {_as_text(item) for item in permission if _as_text(item)}
        if target not in allowed:
            return (
                f"动作「{hit.get('动作类别', '')}」缺少管道 {target} 的交付权限，"
                "不得改投其他管道"
            )
        # 显式声明了可投递管道时，以调用方配置为准，不再按动作类别兜底推断。
        return None
    allowed_targets = DELIVERY_PERMISSIONS.get(_as_text(hit.get("动作类别")))
    if allowed_targets is not None and target not in allowed_targets:
        return (
            f"动作「{hit.get('动作类别', '')}」缺少管道 {target} 的交付权限，"
            "不得改投其他管道"
        )
    return None


def _next_number(rows: list[dict[str, Any]], field: str, prefix: str) -> str:
    biggest = 0
    for row in rows:
        value = _as_text(row.get(field))
        tail = value.rsplit("-", 1)[-1]
        if tail.isdigit():
            biggest = max(biggest, int(tail))
    return f"{prefix}-{biggest + 1:04d}"


def _copy(row: dict[str, Any] | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------
# 服务
# ---------------------------------------------------------------------------
class UserActionDeliveryService:
    """用户动作交付批次与归属清单的业务实现。"""

    # -- 查询 ---------------------------------------------------------------
    @property
    def ownership_rows(self) -> list[dict[str, Any]]:
        return store.rows(OWNERSHIP_MODULE)

    @property
    def batch_rows(self) -> list[dict[str, Any]]:
        return store.rows(BATCH_MODULE)

    def get_ownership(self, ownership_id: Any) -> dict[str, Any] | None:
        for row in self.ownership_rows:
            if row.get("id") == ownership_id or row.get("归属编号") == ownership_id:
                return row
        return None

    def get_batch(self, batch_id: Any) -> dict[str, Any] | None:
        for row in self.batch_rows:
            if row.get("id") == batch_id or row.get("批次编号") == batch_id:
                return row
        return None

    def find_ownership(
        self,
        user: str | None = None,
        event: str | None = None,
        category: str | None = None,
        *,
        include_rejected: bool = True,
    ) -> dict[str, Any] | None:
        matched: list[dict[str, Any]] = []
        for row in self.ownership_rows:
            if user is not None and _as_text(row.get("用户ID")) != _as_text(user):
                continue
            if event is not None and _as_text(row.get("业务事件")) != _as_text(event):
                continue
            if category is not None and _as_text(row.get("动作类别")) != _as_text(category):
                continue
            matched.append(row)
        for row in matched:
            # 有效归属优先返回，避免补送时查到已判死的那一份。
            if row.get("交付状态") in ACTIVE_STATUSES:
                return row
        if include_rejected and matched:
            return matched[0]
        return None

    def _find_active_ownership(self, key: tuple[str, str, str]) -> dict[str, Any] | None:
        user, event, category = key
        for row in self.ownership_rows:
            if (
                _as_text(row.get("用户ID")) == user
                and _as_text(row.get("业务事件")) == event
                and _as_text(row.get("动作类别")) == category
                and row.get("交付状态") in ACTIVE_STATUSES
            ):
                return row
        return None

    def _find_batch_for_ownership(self, ownership: dict[str, Any]) -> dict[str, Any] | None:
        ownership_no = _as_text(ownership.get("归属编号"))
        for row in self.batch_rows:
            if _as_text(row.get("归属编号")) == ownership_no:
                return row
        return None

    def find_by_receipt(self, receipt: str | None) -> dict[str, Any] | None:
        receipt = _as_text(receipt)
        if not receipt:
            return None
        for row in self.batch_rows:
            if _as_text(row.get("交付回执")) == receipt:
                return row
        return None

    def find_delivery_by_receipt(self, receipt: str | None) -> dict[str, Any] | None:
        return self.find_by_receipt(receipt)

    lookup_receipt = find_by_receipt

    def receipt_order(
        self, receipt: str | None, pipeline: str | None = None
    ) -> str | None:
        batch = self.find_by_receipt(receipt)
        if batch is None or batch.get("投递状态") != STATUS_DELIVERED:
            return None
        if pipeline is not None and _as_text(batch.get("目标管道")) != _as_text(pipeline):
            return None
        return _as_text(batch.get("订单编号")) or None

    def list_batches(
        self,
        *,
        user: str | None = None,
        event: str | None = None,
        category: str | None = None,
        order: str | None = None,
        pipeline: str | None = None,
        status: str | None = None,
        receipt: str | None = None,
        page: int = 1,
        size: int = 200,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(self.batch_rows)
        rows = self._filter(rows, user, event, category, order, pipeline, status, receipt)
        total = len(rows)
        start = (page - 1) * size
        return [dict(row) for row in rows[start:start + size]], total

    list_deliveries = list_batches

    def list_ownership(
        self,
        *,
        user: str | None = None,
        event: str | None = None,
        category: str | None = None,
        order: str | None = None,
        pipeline: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 200,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._filter(
            list(self.ownership_rows), user, event, category, order, pipeline,
            status, None,
        )
        total = len(rows)
        start = (page - 1) * size
        return [dict(row) for row in rows[start:start + size]], total

    @staticmethod
    def _filter(
        rows: list[dict[str, Any]],
        user: str | None,
        event: str | None,
        category: str | None,
        order: str | None,
        pipeline: str | None,
        status: str | None,
        receipt: str | None,
    ) -> list[dict[str, Any]]:
        if user is not None:
            rows = [r for r in rows if _as_text(r.get("用户ID")) == _as_text(user)]
        if event is not None:
            rows = [r for r in rows if _as_text(r.get("业务事件")) == _as_text(event)]
        if category is not None:
            rows = [r for r in rows if _as_text(r.get("动作类别")) == _as_text(category)]
        if order is not None:
            rows = [r for r in rows if _as_text(r.get("订单编号")) == _as_text(order)]
        if pipeline is not None:
            rows = [r for r in rows if _as_text(r.get("目标管道")) == _as_text(pipeline)]
        if status is not None:
            key = "交付状态" if any("交付状态" in r for r in rows) else "投递状态"
            rows = [r for r in rows if r.get(key) == status]
        if receipt is not None:
            rows = [r for r in rows if _as_text(r.get("交付回执")) == _as_text(receipt)]
        return rows

    def confirmed_deliveries(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.batch_rows if r.get("投递状态") == STATUS_CONFIRMED]

    def pending_retry_deliveries(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.batch_rows if r.get("投递状态") == STATUS_RETRY]

    def delivered(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.batch_rows if r.get("投递状态") == STATUS_DELIVERED]

    # -- 命中登记 -----------------------------------------------------------
    def submit_hit(self, hit: dict[str, Any], *, now: str | None = None) -> dict[str, Any]:
        normalized = _norm_hit(hit)
        missing = _missing_fields(normalized)
        if missing:
            return {
                "ok": False,
                "created": False,
                "duplicate": False,
                "reason": f"缺少必填字段：{'、'.join(missing)}",
                "hit": normalized,
            }
        permission_error = _permission_error(normalized)
        if permission_error:
            return {
                "ok": False,
                "created": False,
                "duplicate": False,
                "reason": permission_error,
                "hit": normalized,
            }

        key = (
            normalized["用户ID"],
            normalized["业务事件"],
            normalized["动作类别"],
        )
        timestamp = _now(now)
        ownership = self._find_active_ownership(key)
        if ownership is not None:
            sources = list(ownership.get("命中来源") or [])
            rule = normalized["规则编号"]
            if rule and rule not in sources:
                sources.append(rule)
            ownership["命中来源"] = sources
            ownership["动作"] = ownership.get("动作") or normalized["动作"]
            ownership["更新时间"] = timestamp
            # 低优先级的命中先到时先落归属；高优先级随后到达且动作尚未确认，
            # 可以接管目标管道。已经确认或已交付的归属不再改投。
            if (
                ownership.get("交付状态") == STATUS_PENDING
                and normalized["目标管道"]
                and normalized["优先级"]
                and (
                    not _as_priority(ownership.get("优先级"))
                    or normalized["优先级"] < _as_priority(ownership.get("优先级"))
                )
            ):
                ownership["目标管道"] = normalized["目标管道"]
                ownership["优先级"] = normalized["优先级"]
                ownership["交付权限"] = normalized["交付权限"]
            return {
                "ok": True,
                "created": False,
                "duplicate": True,
                "deduplicated": True,
                "reason": "同一用户、业务事件与动作类别已存在归属，保留命中来源",
                "hit": normalized,
                "ownership": dict(ownership),
                "归属编号": ownership.get("归属编号"),
                "state": ownership.get("交付状态"),
            }

        ownership = {
            "id": max((int(r.get("id", 0)) for r in self.ownership_rows), default=0) + 1,
            "归属编号": _next_number(self.ownership_rows, "归属编号", "OWN"),
            "用户ID": normalized["用户ID"],
            "业务事件": normalized["业务事件"],
            "动作类别": normalized["动作类别"],
            "订单编号": normalized["订单编号"],
            "动作": normalized["动作"],
            "命中来源": [normalized["规则编号"]] if normalized["规则编号"] else [],
            "优先级": normalized["优先级"],
            "目标管道": normalized["目标管道"],
            "交付权限": normalized["交付权限"],
            "交付状态": STATUS_PENDING,
            "交付批次": None,
            "交付回执": None,
            "领取管道": None,
            "重试次数": 0,
            "失败原因": None,
            "创建时间": timestamp,
            "更新时间": timestamp,
            "pending": True,
            "abnormal": False,
        }
        self.ownership_rows.append(ownership)
        return {
            "ok": True,
            "created": True,
            "duplicate": False,
            "deduplicated": False,
            "reason": "已登记用户动作归属",
            "hit": normalized,
            "ownership": dict(ownership),
            "归属编号": ownership["归属编号"],
            "state": ownership["交付状态"],
        }

    def submit_hits(
        self, hits: Iterable[dict[str, Any]], *, now: str | None = None
    ) -> dict[str, Any]:
        items = list(hits)
        with _LOCK:
            results = [self.submit_hit(hit, now=now) for hit in items]
            ownership: dict[str, dict[str, Any]] = {}
            for result in results:
                row = result.get("ownership")
                if row:
                    ownership[row["归属编号"]] = row
            created = sum(1 for r in results if r.get("created"))
            duplicates = sum(1 for r in results if r.get("duplicate"))
            rejected = [r for r in results if not r.get("ok")]
            batches: list[dict[str, Any]] = []
            for row in ownership.values():
                batch = self._find_batch_for_ownership(row)
                if batch is not None:
                    batches.append(dict(batch))
            return {
                "ok": not rejected or len(rejected) < len(results),
                "hits": len(items),
                "created": created,
                "merged": duplicates,
                "duplicates": duplicates,
                "deduplicated": duplicates,
                "rejected": rejected,
                "accepted": results,
                "ownership": list(ownership.values()),
                "batches": batches,
                "unique": len(ownership),
            }

    # 常见别名：调用方常把这一步叫 ingest/record/route hits。
    ingest_hits = submit_hits
    record_hits = submit_hits
    record_user_action_hits = submit_hits
    submit_user_action_hits = submit_hits

    # -- 路由确认 -----------------------------------------------------------
    def confirm_pending(
        self,
        *,
        ownership_ids: Iterable[Any] | None = None,
        now: str | None = None,
    ) -> list[dict[str, Any]]:
        wanted = set(ownership_ids) if ownership_ids is not None else None
        timestamp = _now(now)
        confirmed: list[dict[str, Any]] = []
        with _LOCK:
            for ownership in self.ownership_rows:
                if wanted is not None and (
                    ownership.get("id") not in wanted
                    and ownership.get("归属编号") not in wanted
                ):
                    continue
                if ownership.get("交付状态") != STATUS_PENDING:
                    continue
                permission_error = _permission_error(ownership)
                if permission_error:
                    ownership["交付状态"] = STATUS_REJECTED
                    ownership["失败原因"] = permission_error
                    ownership["pending"] = False
                    ownership["abnormal"] = True
                    ownership["更新时间"] = timestamp
                    continue
                batch = self._find_batch_for_ownership(ownership)
                if batch is None:
                    batch = {
                        "id": max(
                            (int(r.get("id", 0)) for r in self.batch_rows), default=0
                        ) + 1,
                        "批次编号": _next_number(
                            self.batch_rows, "批次编号", "UAD"
                        ),
                        "归属编号": ownership.get("归属编号"),
                        "用户ID": ownership.get("用户ID"),
                        "业务事件": ownership.get("业务事件"),
                        "动作类别": ownership.get("动作类别"),
                        "订单编号": ownership.get("订单编号"),
                        "命中来源": list(ownership.get("命中来源") or []),
                        "优先级": ownership.get("优先级"),
                        "目标管道": ownership.get("目标管道"),
                        "交付权限": ownership.get("交付权限"),
                        "投递状态": STATUS_CONFIRMED,
                        "交付回执": None,
                        "领取管道": None,
                        "重试次数": 0,
                        "失败原因": None,
                        "创建时间": timestamp,
                        "更新时间": timestamp,
                        "pending": True,
                        "abnormal": False,
                    }
                    self.batch_rows.append(batch)
                else:
                    batch["投递状态"] = STATUS_CONFIRMED
                    batch["目标管道"] = ownership.get("目标管道")
                    batch["命中来源"] = list(ownership.get("命中来源") or [])
                    batch["更新时间"] = timestamp
                    batch["pending"] = True
                ownership["交付状态"] = STATUS_CONFIRMED
                ownership["交付批次"] = batch.get("批次编号")
                ownership["失败原因"] = None
                ownership["pending"] = True
                ownership["abnormal"] = False
                ownership["更新时间"] = timestamp
                confirmed.append(dict(batch))
        return confirmed

    confirm_deliveries = confirm_pending
    confirm_routing = confirm_pending

    # -- 真实投递 -----------------------------------------------------------
    @staticmethod
    def _delivery_permission_error(
        batch: dict[str, Any], ownership: dict[str, Any] | None
    ) -> str | None:
        error = _permission_error(batch)
        if error:
            return error
        if ownership is None:
            return "缺少用户动作归属，动作不得投递"
        # 归属上的权限是权威配置；批次只是它的副本，防止路由确认后权限被收回。
        return _permission_error(
            {
                "目标管道": batch.get("目标管道"),
                "动作类别": ownership.get("动作类别") or batch.get("动作类别"),
                "交付权限": ownership.get("交付权限"),
            }
        )

    def _write_to_pipeline(
        self, batch: dict[str, Any], ownership: dict[str, Any] | None, receipt: str
    ) -> None:
        target = _as_text(batch.get("目标管道"))
        if not target or target.casefold() == "default":
            raise ValueError("缺少目标管道，动作未写入任何管道")
        inbox = store.rows(INBOX_MODULE)
        for row in inbox:
            if (
                _as_text(row.get("交付回执")) == receipt
                and _as_text(row.get("管道")) == target
            ):
                return
        inbox.append(
            {
                "管道": target,
                "订单编号": batch.get("订单编号"),
                "业务事件": batch.get("业务事件"),
                "用户ID": batch.get("用户ID"),
                "动作类别": batch.get("动作类别"),
                "交付回执": receipt,
                "归属编号": batch.get("归属编号"),
                "批次编号": batch.get("批次编号"),
                "状态": "待领取",
                "创建时间": _now(),
            }
        )

    def deliver_pending(
        self,
        *,
        batch_ids: Iterable[Any] | None = None,
        writer: Callable[[dict[str, Any], dict[str, Any] | None, str], Any] | None = None,
        fail_targets: Iterable[str] | None = None,
        now: str | None = None,
    ) -> list[dict[str, Any]]:
        wanted = set(batch_ids) if batch_ids is not None else None
        failing = {_as_text(item) for item in (fail_targets or ())}
        timestamp = _now(now)
        delivered: list[dict[str, Any]] = []
        with _LOCK:
            for batch in self.batch_rows:
                if wanted is not None and (
                    batch.get("id") not in wanted
                    and batch.get("批次编号") not in wanted
                ):
                    continue
                if batch.get("投递状态") not in DELIVERABLE_STATUSES:
                    continue
                ownership = self.get_ownership(batch.get("归属编号"))
                permission_error = self._delivery_permission_error(batch, ownership)
                if permission_error:
                    batch["投递状态"] = STATUS_REJECTED
                    batch["失败原因"] = permission_error
                    batch["pending"] = False
                    batch["abnormal"] = True
                    batch["更新时间"] = timestamp
                    if ownership is not None:
                        ownership["交付状态"] = STATUS_REJECTED
                        ownership["失败原因"] = permission_error
                        ownership["pending"] = False
                        ownership["abnormal"] = True
                        ownership["更新时间"] = timestamp
                    continue

                receipt = _as_text(batch.get("交付回执")) or _next_number(
                    [r for r in self.batch_rows if r.get("交付回执")],
                    "交付回执",
                    "RCPT",
                )
                try:
                    if _as_text(batch.get("目标管道")) in failing:
                        raise RuntimeError("目标管道写入失败")
                    if writer is not None:
                        outcome = writer(batch, ownership, receipt)
                        if outcome is False:
                            raise RuntimeError("目标管道拒绝写入")
                    else:
                        self._write_to_pipeline(batch, ownership, receipt)
                except Exception as exc:  # noqa: BLE001 - 投递失败要落成可重试状态
                    batch["投递状态"] = STATUS_RETRY
                    batch["交付回执"] = None
                    batch["失败原因"] = str(exc) or "目标管道写入失败"
                    batch["重试次数"] = int(batch.get("重试次数") or 0) + 1
                    batch["pending"] = True
                    batch["abnormal"] = True
                    batch["更新时间"] = timestamp
                    if ownership is not None:
                        ownership["交付状态"] = STATUS_RETRY
                        ownership["交付回执"] = None
                        ownership["失败原因"] = batch["失败原因"]
                        ownership["pending"] = True
                        ownership["abnormal"] = True
                        ownership["更新时间"] = timestamp
                    continue

                # 只有写进目标管道，动作才算真实交付完成。
                batch["投递状态"] = STATUS_DELIVERED
                batch["交付回执"] = receipt
                batch["失败原因"] = None
                batch["pending"] = False
                batch["abnormal"] = False
                batch["更新时间"] = timestamp
                if ownership is not None:
                    ownership["交付状态"] = STATUS_DELIVERED
                    ownership["交付批次"] = batch.get("批次编号")
                    ownership["交付回执"] = receipt
                    ownership["失败原因"] = None
                    ownership["pending"] = False
                    ownership["abnormal"] = False
                    ownership["更新时间"] = timestamp
                delivered.append(dict(batch))
        return delivered

    deliver_confirmed = deliver_pending
    dispatch_deliveries = deliver_pending
    write_deliveries = deliver_pending
    retry_pending = deliver_pending

    def process_hits(
        self,
        hits: Iterable[dict[str, Any]],
        *,
        confirm: bool = True,
        deliver: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        outcome = self.submit_hits(hits)
        accepted_ids = {
            item.get("归属编号")
            for item in outcome.get("accepted", [])
            if item.get("归属编号")
        }
        with _LOCK:
            if confirm:
                outcome["confirmed"] = self.confirm_pending()
            if deliver:
                outcome["delivered"] = self.deliver_pending(**kwargs)
            outcome["batches"] = [
                dict(r) for r in self.batch_rows
                if r.get("归属编号") in accepted_ids
            ]
            outcome["ownership"] = [
                dict(r) for r in self.ownership_rows
                if r.get("归属编号") in accepted_ids
            ]
            outcome["unique_deliveries"] = len(
                [r for r in outcome["batches"] if r.get("投递状态") == STATUS_DELIVERED]
            )
            return outcome

    route_hits = process_hits
    run_delivery = process_hits

    # -- 接收管道领取 / 回执反查 -------------------------------------------
    def claim_delivery(
        self,
        pipeline: str | None = None,
        *,
        receipt: str | None = None,
        order: str | None = None,
        user: str | None = None,
        event: str | None = None,
        category: str | None = None,
        now: str | None = None,
    ) -> dict[str, Any]:
        pipeline = _as_text(pipeline)
        timestamp = _now(now)
        with _LOCK:
            if receipt:
                batch = self.find_by_receipt(receipt)
                if batch is None:
                    return self._claim_failure("交付回执不存在")
                if batch.get("投递状态") != STATUS_DELIVERED:
                    return self._claim_failure("动作尚未写入目标管道，不能算交付完成")
                owner_pipeline = _as_text(batch.get("目标管道"))
                if pipeline and owner_pipeline != pipeline:
                    return self._claim_failure(
                        "该交付回执不属于当前管道，不能据此领取订单"
                    )
                claimed_by = _as_text(batch.get("领取管道"))
                if claimed_by and claimed_by != (pipeline or owner_pipeline):
                    return self._claim_failure("该订单已由其他管道接单")
                return self._claim_success(batch, pipeline or owner_pipeline, timestamp)

            order_text = _as_text(order)
            if not order_text:
                return self._claim_failure("缺少订单编号或交付回执")

            def matches(row: dict[str, Any]) -> bool:
                if _as_text(row.get("订单编号")) != order_text:
                    return False
                if user is not None and _as_text(row.get("用户ID")) != _as_text(user):
                    return False
                if event is not None and _as_text(row.get("业务事件")) != _as_text(event):
                    return False
                if category is not None and _as_text(row.get("动作类别")) != _as_text(category):
                    return False
                return True

            all_for_order = [r for r in self.batch_rows if matches(r)]
            if not all_for_order:
                return self._claim_failure("没有找到该订单的交付批次")
            candidates = [
                r for r in all_for_order
                if (not pipeline or _as_text(r.get("目标管道")) == pipeline)
                and r.get("投递状态") == STATUS_DELIVERED
            ]
            if not candidates:
                return self._claim_failure(
                    "该订单的交付目标不是当前管道，补送不能重复领取"
                )
            batch = candidates[0]
            claimed_by = _as_text(batch.get("领取管道"))
            if claimed_by and pipeline and claimed_by != pipeline:
                return self._claim_failure("该订单已由其他管道接单")
            return self._claim_success(
                batch, pipeline or _as_text(batch.get("目标管道")), timestamp
            )

    accept_order = claim_delivery
    take_order = claim_delivery
    claim_order = claim_delivery

    def _claim_success(
        self, batch: dict[str, Any], pipeline: str, timestamp: str
    ) -> dict[str, Any]:
        batch["领取管道"] = pipeline
        batch["更新时间"] = timestamp
        for row in store.rows(INBOX_MODULE):
            if (
                _as_text(row.get("交付回执")) == _as_text(batch.get("交付回执"))
                and _as_text(row.get("管道")) == pipeline
            ):
                row["状态"] = "处理中"
                row["领取时间"] = timestamp
        return {
            "ok": True,
            "pipeline": pipeline,
            "订单编号": batch.get("订单编号"),
            "order": batch.get("订单编号"),
            "交付回执": batch.get("交付回执"),
            "receipt": batch.get("交付回执"),
            "业务事件": batch.get("业务事件"),
            "batch": dict(batch),
            "message": "接收管道已领取订单",
        }

    @staticmethod
    def _claim_failure(reason: str) -> dict[str, Any]:
        return {
            "ok": False,
            "pipeline": None,
            "订单编号": None,
            "order": None,
            "交付回执": None,
            "receipt": None,
            "message": reason,
            "reason": reason,
        }

    # -- 并发场景辅助 -------------------------------------------------------
    def split_concurrent_deliveries(self) -> dict[str, list[dict[str, Any]]]:
        """把路由已确认与待重试的投递分开，便于并发消费。"""
        return {
            "已确认": self.confirmed_deliveries(),
            "待重试": self.pending_retry_deliveries(),
            "已交付": self.delivered(),
        }

    concurrent_view = split_concurrent_deliveries

    def inbox(self, pipeline: str | None = None) -> list[dict[str, Any]]:
        rows = [dict(row) for row in store.rows(INBOX_MODULE)]
        if pipeline is not None:
            rows = [r for r in rows if _as_text(r.get("管道")) == _as_text(pipeline)]
        return rows


service = UserActionDeliveryService()

# 模块级别名，便于调用方按自己的习惯命名。
submit_hits = service.submit_hits
submit_hit = service.submit_hit
ingest_hits = service.submit_hits
record_hits = service.submit_hits
confirm_pending = service.confirm_pending
confirm_deliveries = service.confirm_pending
deliver_pending = service.deliver_pending
deliver_confirmed = service.deliver_pending
process_hits = service.process_hits
claim_delivery = service.claim_delivery
claim_order = service.claim_delivery
take_order = service.claim_delivery
find_by_receipt = service.find_by_receipt
lookup_receipt = service.find_by_receipt
receipt_order = service.receipt_order
list_batches = service.list_batches
list_deliveries = service.list_batches
list_ownership = service.list_ownership
pending_retry_deliveries = service.pending_retry_deliveries
confirmed_deliveries = service.confirmed_deliveries
