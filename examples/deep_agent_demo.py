"""A Deep Agent on the sandbox (§8.4). Needs Docker, a generated prod catalog (python -m dbrix.generator), and an
Anthropic API key. Run: uv run python examples/deep_agent_demo.py"""

from langgraph.types import Command

from dbrix.config import OperatingMode
from dbrix.deepagents import create_sandboxed_deep_agent

agent = create_sandboxed_deep_agent(model="claude-sonnet-5-5", mode=OperatingMode.READ_WRITE, agent_id="analyst")
config = {"configurable": {"thread_id": "demo-1"}}

state = agent.invoke(
    {
        "messages": [
            {
                "role": "user",
                "content": "Which creator had the best engagement rate per brand last month? "
                "Save the answer as prod_catalog.gold.top_creators.",
            }
        ]
    },
    config,
)
while state.get("__interrupt__"):  # a write waits for approval (§8.3)
    for pending in state["__interrupt__"]:
        print(pending.value["action_requests"][0]["description"])
    answer = input("approve? [y/N] ").strip().lower()
    decision = {"type": "approve"} if answer == "y" else {"type": "reject", "message": "not approved"}
    state = agent.invoke(Command(resume={"decisions": [decision] * len(state["__interrupt__"])}), config)

print(state["messages"][-1].content)
