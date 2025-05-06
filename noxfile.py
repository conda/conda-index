import nox


@nox.session(venv_backend="conda")
@nox.parametrize(
    "python",
    [(python) for python in ("3.9", "3.10", "3.11", "3.12", "3.13")],
)
def tests(session):
    session.conda_install("conda", "conda-build")
    session.install("-e", ".[test]")
    session.run("pytest")
