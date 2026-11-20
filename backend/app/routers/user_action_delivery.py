"""用户动作交付接口：命中登记、路由确认、真实投递与接收管道领取。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, PageResult
from app.services.user_action_delivery import service

router = APIRouter(prefix="/api/user_action_delivery", tags=["用户动作交付"])


@router.get("/deliveries", response_model=PageResult[dict])
def list_deliveries(
    user: str | None = Query(default=None, description="用户ID"),
    event: str | None = Query(default=None, description="业务事件"),
    category: str | None = Query(default=None, description="动作类别"),
    order: str | None = Query(default=None, description="订单编号"),
    pipeline: str | None = Query(default=None, description="目标管道"),
    status: str | None = Query(default=None, description="待投递、已确认、待重试、已交付、无法投递"),
    receipt: str | None = Query(default=None, description="交付回执"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=500),
) -> PageResult[dict]:
    """查看用户动作交付批次；补送不会新增第二条有效交付。"""
    items, total = service.list_batches(
        user=user, event=event, category=category, order=order,
        pipeline=pipeline, status=status, receipt=receipt, page=page, size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/ownership", response_model=PageResult[dict])
def list_ownership(
    user: str | None = Query(default=None),
    event: str | None = Query(default=None),
    category: str | None = Query(default=None),
    order: str | None = Query(default=None),
    pipeline: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=500),
) -> PageResult[dict]:
    """查看归属清单：同一用户、业务事件、动作类别只有一份有效归属。"""
    items, total = service.list_ownership(
        user=user, event=event, category=category, order=order,
        pipeline=pipeline, status=status, page=page, size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/receipt/{receipt}", response_model=dict)
def get_by_receipt(receipt: str) -> dict:
    """接收管道凭原交付回执反查订单。"""
    batch = service.find_by_receipt(receipt)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"交付回执 {receipt} 不存在")
    return {"ok": True, "batch": batch, "订单编号": batch.get("订单编号"), "交付回执": receipt}


@router.post("/hits", response_model=dict)
def submit_hits(payload: list[dict] | dict) -> dict:
    """把用户级命中送入路由：按用户、业务事件、动作类别判定归属。"""
    hits = payload.get("hits", []) if isinstance(payload, dict) else payload
    if not hits:
        return {"ok": False, "message": "没有可路由的用户命中", "accepted": []}
    result = service.submit_hits(hits)
    result["message"] = (
        f"命中 {result['hits']} 条，登记归属 {result['created']} 份，"
        f"合并重复 {result['merged']} 条"
    )
    return result


@router.post("/confirm", response_model=dict)
def confirm() -> dict:
    """事件路由确认待投递归属，写入用户动作交付批次。"""
    batches = service.confirm_pending()
    return {"ok": True, "confirmed": batches, "message": f"已确认 {len(batches)} 份交付批次"}


@router.post("/deliver", response_model=dict)
def deliver(payload: dict | None = None) -> dict:
    """把已确认或待重试的批次真实写入目标管道；只有写成功才生成交付回执。"""
    payload = payload or {}
    failed_targets = payload.get("fail_targets") or []
    batches = service.deliver_pending(fail_targets=failed_targets)
    return {
        "ok": True,
        "delivered": batches,
        "pending_retry": service.pending_retry_deliveries(),
        "message": f"已交付 {len(batches)} 份动作",
    }


@router.post("/process", response_model=dict)
def process(payload: list[dict] | dict) -> dict:
    """一次完成命中登记、路由确认与真实投递。"""
    hits = payload.get("hits", []) if isinstance(payload, dict) else payload
    if not hits:
        return {"ok": False, "message": "没有可路由的用户命中", "accepted": []}
    result = service.process_hits(hits)
    return result


@router.post("/claims", response_model=dict)
def claim(payload: dict) -> ActionResult:
    """接收管道领取订单；已接单的管道也可以凭原交付回执反查。"""
    result = service.claim_delivery(
        payload.get("pipeline") or payload.get("管道"),
        receipt=payload.get("receipt") or payload.get("交付回执"),
        order=payload.get("order") or payload.get("订单编号"),
        user=payload.get("user") or payload.get("用户ID"),
        event=payload.get("event") or payload.get("业务事件"),
        category=payload.get("category") or payload.get("动作类别"),
    )
    return ActionResult(ok=bool(result.get("ok")), message=result.get("message", ""), entry=result)


@router.get("/inbox", response_model=dict)
def inbox(pipeline: str | None = Query(default=None)) -> dict:
    """查看目标管道已经收到的动作。"""
    return {"ok": True, "items": service.inbox(pipeline)}
