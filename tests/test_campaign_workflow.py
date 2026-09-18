import asyncio
from types import SimpleNamespace

from app.domains.campaigns import orchestrator
from app.domains.campaigns.models import WorkflowStepStatus


class DummyResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class DummyDB:
    def __init__(self, candidate, campaign, template=None):
        self.candidate = candidate
        self.campaign = campaign
        self.template = template
        self.added = []
        self.committed = 0
        self.calls = 0

    async def execute(self, stmt):
        self.calls += 1
        if self.calls == 1:
            return DummyResult(self.candidate)
        if self.calls == 2:
            return DummyResult(self.campaign)
        if self.calls == 3:
            return DummyResult(self.template)
        return DummyResult(None)

    def add(self, obj):
        self.added.append(obj)

    async def commit(self):
        self.committed += 1


def test_on_step_completed_enqueues_screening_for_automatic_workflow(monkeypatch):
    candidate = SimpleNamespace(id="11111111-1111-4111-8111-111111111111", campaign_id="22222222-2222-4222-8222-222222222222", workflow_step=None, step_status=WorkflowStepStatus.COMPLETED)
    campaign = SimpleNamespace(id="22222222-2222-4222-8222-222222222222", workflow_template_id="33333333-3333-4333-8333-333333333333")
    template = SimpleNamespace(template={
        "steps": {
            "document_extraction": {
                "next": "document_screening",
                "execution_mode": "AUTOMATIC",
            }
        }
    })

    calls = {}

    async def fake_enqueue(*, task_name, campaign_id, candidate_id=None):
        calls["args"] = {
            "task_name": task_name,
            "campaign_id": campaign_id,
            "candidate_id": candidate_id,
        }

    monkeypatch.setattr(orchestrator, "enqueue_workflow_task", fake_enqueue)

    db = DummyDB(candidate, campaign, template)
    asyncio.run(orchestrator.on_step_completed(db, candidate.id, "document_extraction", {"match_score": 85}))

    assert calls["args"]["campaign_id"] == candidate.campaign_id
    assert calls["args"]["candidate_id"] == candidate.id
    assert candidate.workflow_step == "document_screening"
    assert candidate.step_status == WorkflowStepStatus.PENDING
