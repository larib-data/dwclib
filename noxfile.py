import nox

locations = ["dwclib"]
python_versions=['3.8']

@nox.session(python=python_versions)
def lint(session):
    args = session.posargs or locations
    session.install(
        "flake8",
        "flake8-bandit",
    )
    session.run("flake8", *args)

@nox.session(python=python_versions)
def safety(session):
    session.install("flit","safety")
    session.run("flit","build")
    session.run("safety", "check", "--full-report")
