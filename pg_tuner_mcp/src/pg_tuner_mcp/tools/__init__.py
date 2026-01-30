"""MCP tools for pg_tuner.

This module exports tool routers for the MCP server.
"""

from .collectors import router as collectors_router
from .config import router as config_router
from .workload import router as workload_router

__all__ = [
    "collectors_router",
    "config_router",
    "workload_router",
]
