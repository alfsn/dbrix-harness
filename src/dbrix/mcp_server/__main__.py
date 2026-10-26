"""python -m dbrix.mcp_server [--transport stdio|http] (§8.5). Needs Docker and a generated prod catalog."""

import argparse

import uvicorn
from docker import from_env

from dbrix.config import load_config
from dbrix.lifecycle.manager import SandboxManager
from dbrix.mcp_server import create_server, http_app

parser = argparse.ArgumentParser(prog="python -m dbrix.mcp_server")
parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
args = parser.parse_args()
cfg = load_config()
server = create_server(SandboxManager(cfg, from_env()))
if args.transport == "stdio":
    server.run()
else:
    uvicorn.run(http_app(server, cfg.mcp), host=cfg.mcp.host, port=cfg.mcp.port)
