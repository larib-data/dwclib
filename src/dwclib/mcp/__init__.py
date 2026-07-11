try:
    from .server import main, mcp
except ModuleNotFoundError as e:
    if e.name == "fastmcp" or (e.name or "").startswith("fastmcp."):
        raise ModuleNotFoundError(
            "dwclib.mcp requires the optional 'mcp' extra. "
            "Install it with: pip install dwclib[mcp]"
        ) from e
    raise

__all__ = ["mcp", "main"]
