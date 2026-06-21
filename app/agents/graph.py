from langgraph.graph import END, StateGraph

from app.agents.state import AgentState
from app.agents.nodes import (
    analyse_root_cause,
    classify_failure,
    generate_fix,
    review_security,
    score_confidence,
    should_block_high_risk,
    write_pr_description,
)


def _handle_blocked(state: AgentState) -> AgentState:
    trace = state.get("agent_trace", [])
    trace.append("BLOCKED: security risk too high — fix not stored")
    return {
        **state,
        "confidence_score": 0,
        "confidence_reasoning": "Fix blocked by security reviewer.",
        "error": (
            f"Proposed fix blocked by security reviewer "
            f"(risk score {state.get('security_risk_score')}/10). "
            f"Findings: {'; '.join(state.get('security_findings', []))}"
        ),
        "agent_trace": trace,
    }


def build_graph() -> StateGraph:
    graph = StateGraph(AgentState)

    graph.add_node("classify", classify_failure)
    graph.add_node("root_cause", analyse_root_cause)
    graph.add_node("fix", generate_fix)
    graph.add_node("security", review_security)
    graph.add_node("pr_writer", write_pr_description)
    graph.add_node("confidence", score_confidence)
    graph.add_node("blocked", _handle_blocked)

    graph.set_entry_point("classify")
    graph.add_edge("classify", "root_cause")
    graph.add_edge("root_cause", "fix")
    graph.add_edge("fix", "security")
    graph.add_conditional_edges(
        "security",
        should_block_high_risk,
        {"block": "blocked", "approve": "pr_writer"},
    )
    # confidence scorer runs after PR writer (or after blocked) before END
    graph.add_edge("pr_writer", "confidence")
    graph.add_edge("confidence", END)
    graph.add_edge("blocked", END)

    return graph.compile()


remediation_graph = build_graph()
