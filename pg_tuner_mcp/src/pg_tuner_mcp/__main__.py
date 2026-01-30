"""Allow running pg_tuner_mcp as a module: python -m pg_tuner_mcp"""

from .server import main

if __name__ == "__main__":
    main()
