"""事件路由接口：维护路由规则，覆盖启用路由、停用路由、归档规则等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.event_router import EventRouterService
from app.services.user_action_delivery import service as delivery_service

router = APIRouter(prefix="/api/event_router", tags=["事件路由"])

service = EventRouterService()

LIST_FIELDS = ["规则编号", "事件类型", "匹配条件", "目标管道", "优先级", "分区键", "重试上限", "DLQ策略"]
STATUSES = ["草稿", "路由中", "已停用", "已被拒"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按规则编号检索"),
    status: str | None = Query(default=None, description="草稿、路由中、已停用、已被拒"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、规则编号关键字与状态过滤事件路由列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="event_router.csv"'})


@router.get("/deliveries", response_model=PageResult[dict])
def list_deliveries(
    user: str | None = Query(default=None, description="用户ID"),
    event: str | None = Query(default=None, description="业务事件"),
    category: str | None = Query(default=None, description="动作类别"),
    order: str | None = Query(default=None, description="订单编号"),
    pipeline: str | None = Query(default=None, description="目标管道"),
    status: str | None = Query(default=None, description="待投递、已确认、待重试、已交付、无法投递"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=500),
) -> PageResult[dict]:
    """按用户、业务事件、动作类别查看事件路由写入的交付批次。"""
    items, total = service.list_deliveries(
        user=user, event=event, category=category, order=order,
        pipeline=pipeline, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/ownership", response_model=PageResult[dict])
def list_ownership(
    user: str | None = Query(default=None),
    event: str | None = Query(default=None),
    category: str | None = Query(default=None),
    order: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=500),
) -> PageResult[dict]:
    """查看用户动作归属清单，含命中来源与投递状态回写。"""
    items, total = service.list_ownership(
        user=user, event=event, category=category, order=order,
        status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/receipt/{receipt}", response_model=dict)
def lookup_receipt(receipt: str) -> dict:
    """接收管道凭原交付回执查回订单。"""
    batch = service.lookup_receipt(receipt)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"交付回执 {receipt} 不存在")
    return {"ok": True, "batch": batch, "订单编号": batch.get("订单编号"), "交付回执": receipt}


@router.post("/hits", response_model=dict)
def route_hits(payload: list[dict] | dict) -> dict:
    """用户级命中进入事件路由：同一用户同类动作只写一份，命中来源全部保留。"""
    hits = payload.get("hits", []) if isinstance(payload, dict) else payload
    if not hits:
        return {"ok": False, "message": "没有可路由的用户命中", "accepted": []}
    outcome = delivery_service.process_hits(hits)
    outcome["message"] = (
        f"命中 {outcome['hits']} 条，归属 {outcome['unique']} 份，"
        f"合并重复 {outcome['duplicates']} 条，交付 {len(outcome.get('delivered', []))} 份"
    )
    return outcome


@router.post("/claims", response_model=ActionResult)
def claim_order(payload: dict) -> ActionResult:
    """接收管道领取订单：优先用原交付回执反查，补送不能绕过归属再领一次。"""
    result = service.claim_order(
        str(payload.get("pipeline") or payload.get("管道") or ""),
        receipt=payload.get("receipt") or payload.get("交付回执"),
        order=payload.get("order") or payload.get("订单编号"),
        user=payload.get("user") or payload.get("用户ID"),
        event=payload.get("event") or payload.get("业务事件"),
        category=payload.get("category") or payload.get("动作类别"),
    )
    return ActionResult(ok=bool(result.get("ok")), message=result.get("message", ""), entry=result)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条路由规则明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"路由规则 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条路由规则，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="路由规则已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条路由规则执行启用路由、停用路由、归档规则；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
