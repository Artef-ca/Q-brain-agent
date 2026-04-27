from app.governance.policy_registry import TablePolicy
from app.agents.policy_agent import PolicyAgent

class DummyRegistry:
    def get(self, ds, tb):
        return TablePolicy(ds, tb, True, False, True, "g1", False, False, "INTERNAL")

def test_can_synthesize_ok():
    p = PolicyAgent(DummyRegistry())
    ok, reason = p.can_synthesize([DummyRegistry().get("a","b")])
    assert ok
    assert reason == "SYNTHESIS_ALLOWED"
