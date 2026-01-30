"""MCP server entry point for pg_tuner_mcp.

This server provides tools for PostgreSQL performance analysis
and tuning recommendations via the MCP protocol.
"""

import argparse
import logging
import os
import sys

from fastmcp import FastMCP

from . import __version__
from .tools import collectors_router, config_router, workload_router

# Configure logging based on environment variable
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("pg_tuner_mcp")

# Create the MCP server
mcp = FastMCP("pg-tuner")

# Mount tool servers
mcp.mount(collectors_router)
mcp.mount(config_router)
mcp.mount(workload_router)


# Add a simple ping tool at the root level
@mcp.tool()
async def ping() -> dict:
    """Check if the pg_tuner MCP server is running and responsive.

    Returns:
        Status response with server version.
    """
    return {"status": "ok", "version": __version__}


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="pg_tuner MCP server for PostgreSQL tuning analysis"
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"pg_tuner_mcp {__version__}",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Set logging level (default: INFO, or LOG_LEVEL env var)",
    )

    args = parser.parse_args()

    # Update logging level if specified via command line
    if args.log_level:
        logging.getLogger().setLevel(getattr(logging, args.log_level))

    logger.info(f"Starting pg_tuner_mcp server v{__version__}")

    # Run the server
    mcp.run()


if __name__ == "__main__":
    main()
