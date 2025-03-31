from contextlib import contextmanager
from pathlib import Path
from subprocess import run
from functools import partial
import sqlalchemy
from sqlalchemy import text
import shutil

BASE = Path(__file__).parents[1]

USERNAME = "conda_index_test"
DBNAME = "conda_index_test"


@contextmanager
def postgresql_fixture():
    """
    Run a local postgresql server for testing.
    """
    run(
        [
            "initdb",
            "-D",
            "conda_index_db",
        ],
        cwd=BASE,
    )
    run(["pg_ctl", "-D", "conda_index_db", "start"], check=True, cwd=BASE)
    run(["createuser", "-d", USERNAME], check=True, cwd=BASE)
    run(["createdb", "--owner", USERNAME, DBNAME], check=True)

    engine = sqlalchemy.create_engine(f"postgresql://{USERNAME}@localhost/{DBNAME}")

    print(list(engine.connect().execute(text("SELECT 1"))))

    yield engine

    run(["pg_ctl", "-D", "conda_index_db", "stop"], cwd=BASE, check=True)
    shutil.rmtree(BASE / "conda_index_db")


if __name__ == "__main__":
    with postgresql_fixture():
        print("Used postgresql")
