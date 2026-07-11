import tempfile

import nox
from nox.sessions import Session

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["lint", "audit", "docs"]

locations = ["src/dwclib"]
python_versions = ["3.10", "3.11", "3.12", "3.13"]


def uv_sync(session: Session, *args: str) -> None:
    """Sync dependencies from uv.lock into the session's virtualenv.

    Runs ``uv sync`` with ``UV_PROJECT_ENVIRONMENT`` pointed at the nox
    virtualenv so that packages are installed there at the versions
    pinned in uv.lock, rather than into the project's own environment.

    Arguments:
        session: The Session object.
        args: Extra command-line arguments for ``uv sync``.
    """
    session.run_install(
        "uv",
        "sync",
        *args,
        env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location},
    )


@nox.session(python=python_versions)
def lint(session: Session) -> None:
    """Lint the source with flake8 and its plugins."""
    uv_sync(session, "--only-group", "lint")
    session.run("flake8", *(session.posargs or locations))


@nox.session
def audit(session: Session) -> None:
    """Scan the locked runtime dependencies for known vulnerabilities."""
    uv_sync(session, "--only-group", "audit")
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "uv",
            "export",
            "--no-hashes",
            "--no-default-groups",
            "--no-emit-project",
            f"--output-file={requirements.name}",
            external=True,
        )
        session.run("pip-audit", "-r", requirements.name)


@nox.session
def docs(session: Session) -> None:
    """Build the documentation."""
    uv_sync(session, "--group", "docs")
    session.run("sphinx-build", "docs", "docs/_build")
