import shutil
from contextlib import contextmanager
from pathlib import Path
from subprocess import run

import sqlalchemy
from sqlalchemy import text

BASE = Path(__file__).parents[1]

USERNAME = "conda_index_test"
DBNAME = "conda_index_test"
DBDIR = "conda_index_db"


def postgresql_fixture(BASE: Path):
    """
    Run a local postgresql server for testing.
    """

    if not (BASE / DBDIR).exists():
        run(
            ["initdb", "-D", DBDIR],
            cwd=BASE,
        )

    try:
        run(["pg_ctl", "-D", DBDIR, "start"], check=True, cwd=BASE)
        run(["createuser", "-d", USERNAME], check=False, cwd=BASE)
        run(["createdb", "--owner", USERNAME, DBNAME], check=False)

        engine = sqlalchemy.create_engine(f"postgresql://{USERNAME}@localhost/{DBNAME}")

        print(list(engine.connect().execute(text("SELECT 1"))))

        yield engine

    finally:
        run(["pg_ctl", "-D", DBDIR, "stop"], cwd=BASE, check=True)

    shutil.rmtree(BASE / DBDIR)


if __name__ == "__main__":
    with contextmanager(postgresql_fixture)(BASE) as p:
        print("Used postgresql")
        # p.url is the db url
