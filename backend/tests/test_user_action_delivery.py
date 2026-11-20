"""用户动作交付：动作去重、交付批次、回执反查与并发投递行为。"""
from __future__ import annotations

import threading

from app.services.pipeline_orchestrator import run_rule_router_pipeline
from app.services.user_action_delivery import service
from app.store import store


def _hit(**overrides):
    hit = {
        "用户ID": "U1001",
        "业务事件": "EVT-ORD-008",
        "订单编号": "ORD-008",
        "动作": "标记高价值用户",
        "动作类别": "用户标记",
        "规则编号": "RULE-HIGH-VALUE",
        "目标管道": "pipeline-user-profile",
        "优先级": 1,
    }
    hit.update(overrides)
    return hit


def _deliver_all(hits):
    outcome = service.process_hits(hits)
    return outcome


class TestActionDedup:
    def test_same_user_same_event_same_category_written_once(self):
        outcome = _deliver_all([
            _hit(规则编号="RULE-HIGH-VALUE", 优先级=1),
            _hit(规则编号="RULE-FREQ-USER", 优先级=3),
        ])
        assert outcome["hits"] == 2
        assert outcome["unique"] == 1
        assert outcome["duplicates"] == 1
        assert len(outcome["batches"]) == 1
        sources = outcome["ownership"][0]["命中来源"]
        assert sources == ["RULE-HIGH-VALUE", "RULE-FREQ-USER"]
        actual = [r for r in service.batch_rows if r["用户ID"] == "U1001"]
        assert len(actual) == 1

    def test_other_users_still_processed(self):
        outcome = _deliver_all([
            _hit(用户ID="U1001"),
            _hit(用户ID="U1004"),
        ])
        assert outcome["unique"] == 2
        assert len(outcome["batches"]) == 2
        assert {r["用户ID"] for r in service.batch_rows} == {"U1001", "U1004"}

    def test_different_business_event_is_a_new_action(self):
        outcome = _deliver_all([
            _hit(用户ID="U1", 业务事件="E1", 订单编号="O1"),
            _hit(用户ID="U1", 业务事件="E2", 订单编号="O2"),
            _hit(用户ID="U1", 业务事件="E1", 订单编号="O1"),
        ])
        assert outcome["unique"] == 2
        assert outcome["duplicates"] == 1
        assert len(service.batch_rows) == 2

    def test_different_action_category_is_a_new_action(self):
        outcome = _deliver_all([
            _hit(动作类别="用户标记"),
            _hit(动作类别="订单标记", 目标管道="pipeline-order-process"),
        ])
        assert outcome["unique"] == 2

    def test_resend_does_not_create_second_delivery(self):
        _deliver_all([_hit(), _hit(规则编号="RULE-FREQ-USER")])
        first = [dict(r) for r in service.batch_rows]
        outcome = _deliver_all([_hit(), _hit(规则编号="RULE-FREQ-USER")])
        assert outcome["unique"] == 1
        assert outcome["duplicates"] == 2
        assert [dict(r) for r in service.batch_rows] == first


class TestOwnershipKey:
    def test_ownership_key_is_user_event_category(self):
        _deliver_all([_hit()])
        ownership = service.find_ownership("U1001", "EVT-ORD-008", "用户标记")
        assert ownership is not None
        assert ownership["交付状态"] == "已交付"
        assert service.find_ownership("U1002", "EVT-ORD-008", "用户标记") is None

    def test_delivery_status_written_back_to_ownership(self):
        service.submit_hit(_hit())
        ownership = service.find_ownership("U1001", "EVT-ORD-008", "用户标记")
        assert ownership["交付状态"] == "待投递"
        service.confirm_pending()
        assert ownership["交付状态"] == "已确认"
        service.deliver_pending()
        assert ownership["交付状态"] == "已交付"
        assert ownership["交付回执"]
        assert ownership["交付批次"]


class TestPriorityArrival:
    def test_low_priority_first_high_priority_takes_over_before_confirm(self):
        low = _hit(规则编号="R-LOW", 优先级=5, 目标管道="pipeline-order-process")
        high = _hit(规则编号="R-HIGH", 优先级=1, 目标管道="pipeline-user-profile")
        assert service.submit_hit(low)["ownership"]["目标管道"] == "pipeline-order-process"
        outcome = service.submit_hit(high)
        ownership = outcome["ownership"]
        assert ownership["交付状态"] == "待投递"
        assert ownership["目标管道"] == "pipeline-user-profile"
        assert ownership["命中来源"] == ["R-LOW", "R-HIGH"]

        service.confirm_pending()
        service.deliver_pending()
        batches = [r for r in service.batch_rows if r["订单编号"] == "ORD-008"]
        assert len(batches) == 1
        assert batches[0]["目标管道"] == "pipeline-user-profile"

    def test_confirmed_delivery_is_not_rerouted(self):
        service.submit_hit(_hit(规则编号="R-LOW", 优先级=5, 目标管道="pipeline-order-process"))
        service.confirm_pending()
        outcome = service.submit_hit(
            _hit(规则编号="R-HIGH", 优先级=1, 目标管道="pipeline-user-profile")
        )
        assert outcome["ownership"]["目标管道"] == "pipeline-order-process"
        assert outcome["ownership"]["交付状态"] == "已确认"


class TestConcurrentDelivery:
    def test_confirmed_and_retry_are_separate(self):
        service.submit_hit(_hit(用户ID="U1", 订单编号="O1"))
        service.submit_hit(_hit(用户ID="U2", 订单编号="O2", 目标管道="pipeline-order-process"))
        service.confirm_pending()
        service.deliver_pending(fail_targets=["pipeline-order-process"])
        split = service.split_concurrent_deliveries()
        assert {r["用户ID"] for r in split["已确认"]} == set()
        assert {r["用户ID"] for r in split["已交付"]} == {"U1"}
        assert {r["用户ID"] for r in split["待重试"]} == {"U2"}

    def test_retry_delivers_after_failure(self):
        service.submit_hit(_hit(目标管道="pipeline-order-process"))
        service.confirm_pending()
        assert service.deliver_pending(fail_targets=["pipeline-order-process"]) == []
        batch = service.batch_rows[0]
        assert batch["投递状态"] == "待重试"
        assert batch["交付回执"] is None
        assert service.claim_order("pipeline-order-process", order="ORD-008")["ok"] is False
        delivered = service.deliver_pending()
        assert len(delivered) == 1
        assert batch["投递状态"] == "已交付"
        assert batch["交付回执"]
        assert service.claim_order("pipeline-order-process", order="ORD-008")["ok"] is True

    def test_unwritten_action_is_not_delivered(self):
        service.submit_hit(_hit(目标管道="pipeline-order-process"))
        service.confirm_pending()
        batch = service.batch_rows[0]
        assert batch["投递状态"] == "已确认"
        assert batch["交付回执"] is None
        assert service.receipt_order(batch.get("交付回执")) is None
        assert service.claim_order("pipeline-order-process", order="ORD-008")["ok"] is False

    def test_concurrent_duplicate_submissions_write_one_action(self):
        barrier = threading.Barrier(8)
        errors: list[BaseException] = []

        def worker():
            try:
                barrier.wait()
                service.process_hits([_hit(规则编号="R1"), _hit(规则编号="R2")])
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors
        assert len(service.ownership_rows) == 1
        assert len(service.batch_rows) == 1
        assert service.batch_rows[0]["投递状态"] == "已交付"


class TestDeliveryPermission:
    def test_missing_permission_is_not_delivered_to_other_pipeline(self):
        outcome = service.submit_hit(_hit(目标管道="pipeline-payment-audit"))
        assert outcome["ok"] is False
        assert "交付权限" in outcome["reason"]
        assert service.ownership_rows == []

    def test_explicit_permission_overrides_default(self):
        outcome = service.submit_hit(
            _hit(目标管道="pipeline-user-profile", 交付权限=["pipeline-user-profile"])
        )
        assert outcome["ok"] is True

    def test_denied_permission_marks_ownership_abnormal(self):
        service.submit_hit(_hit())
        service.confirm_pending()
        # 目标管道在确认后失去权限时直接判为无法投递，不改投其他管道。
        service.ownership_rows[0]["交付权限"] = False
        service.deliver_pending()
        assert service.ownership_rows[0]["交付状态"] == "无法投递"
        assert service.batch_rows[0]["投递状态"] == "无法投递"
        assert service.batch_rows[0]["交付回执"] is None
        assert service.inbox() == []


class TestReceiptLookup:
    def test_receipt_returns_order(self):
        _deliver_all([_hit(目标管道="pipeline-order-process")])
        batch = service.batch_rows[0]
        assert service.receipt_order(batch["交付回执"]) == "ORD-008"
        assert service.receipt_order(batch["交付回执"], "pipeline-order-process") == "ORD-008"
        assert service.receipt_order(batch["交付回执"], "pipeline-user-profile") is None

    def test_wrong_pipeline_cannot_claim_resend(self):
        low_first = _deliver_all([
            _hit(规则编号="R-OUT", 目标管道="pipeline-order-process"),
        ])
        assert low_first["unique"] == 1
        service.process_hits([_hit(规则编号="R-OUT", 目标管道="pipeline-order-process")])
        claims = service.claim_order("pipeline-order-process", order="ORD-008")
        assert claims["ok"] is True
        receipt = claims["交付回执"]
        # 另一条管道收到原样补送，也不能领到这笔工作。
        other = service.claim_order("pipeline-user-profile", order="ORD-008")
        assert other["ok"] is False
        # 已接单的管道仍然能凭原交付回执查回订单。
        again = service.claim_order("pipeline-order-process", receipt=receipt)
        assert again["ok"] is True
        assert again["订单编号"] == "ORD-008"

    def test_two_pipelines_two_orders_keep_their_receipts(self):
        _deliver_all([_hit(用户ID="U1", 订单编号="O1", 目标管道="pipeline-order-process")])
        _deliver_all([_hit(用户ID="U2", 订单编号="O2", 目标管道="pipeline-order-process")])
        orders = {
            r["交付回执"]: service.receipt_order(r["交付回执"])
            for r in service.batch_rows
        }
        assert orders == {"RCPT-0001": "O1", "RCPT-0002": "O2"}
        assert service.claim_order("pipeline-order-process", order="O1")["订单编号"] == "O1"
        assert service.claim_order("pipeline-order-process", order="O2")["订单编号"] == "O2"


class TestPipelineIntegration:
    def test_rule_router_pipeline_dedups_user_actions(self):
        result = run_rule_router_pipeline()
        assert result["rules_fired"] == 4
        assert result["total_hits"] == 4
        assert result["unique_actions"] == 3
        assert result["deduplicated_hits"] == 1
        assert result["delivered"] == 3
        u1001 = [r for r in service.ownership_rows if r["用户ID"] == "U1001"]
        assert len(u1001) == 1
        assert set(u1001[0]["命中来源"]) == {"RULE-HIGH-VALUE", "RULE-FREQ-USER"}
        assert u1001[0]["交付状态"] == "已交付"

    def test_rule_router_pipeline_is_idempotent(self):
        run_rule_router_pipeline()
        first = [dict(r) for r in service.batch_rows]
        result = run_rule_router_pipeline()
        assert result["unique_actions"] == 3
        assert [dict(r) for r in service.batch_rows] == first

    def test_pipeline_result_written(self):
        run_rule_router_pipeline()
        assert any(r["pipeline_id"] == "rule_router_orch" for r in store.rows("_pipeline_results"))
