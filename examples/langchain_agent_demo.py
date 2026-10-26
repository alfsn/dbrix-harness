"""The same middleware on plain LangChain create_agent (§8.4): no /catalog or /results files, skills, or subagents.
Run: uv run python examples/langchain_agent_demo.py"""

import docker
from langchain.agents import create_agent
from langgraph.checkpoint.memory import InMemorySaver

from dbrix.config import load_config
from dbrix.lifecycle.manager import SandboxManager
from dbrix.middleware import DbrixSandboxMiddleware
from dbrix.middleware.tools import run_pyspark, run_sql

manager = SandboxManager(load_config(), docker.from_env())
agent = create_agent(
    "claude-sonnet-5-5",
    tools=[run_sql, run_pyspark],
    middleware=[DbrixSandboxMiddleware(manager, agent_id="analyst")],
    checkpointer=InMemorySaver(),
)
state = agent.invoke(
    {
        "messages": [
            {"role": "user", "content": "How many posts did each brand publish? Use prod_catalog.silver.posts."}
        ]
    },
    {"configurable": {"thread_id": "demo-2"}},
)
print(state["messages"][-1].content)
