try:
    from .generic import read_data
    from .numerics import read_numerics
    from .waves import read_waves
except ModuleNotFoundError as e:
    if e.name == "dask" or (e.name or "").startswith("dask."):
        raise ModuleNotFoundError(
            "dwclib.dask requires the optional 'dask' extra. "
            "Install it with: pip install dwclib[dask]"
        ) from e
    raise

__all__ = ["read_numerics", "read_waves", "read_data"]
