import json
import os
import sys
import time
from tkinter import TRUE
from typing import Union

import pandas as pd
import requests as req
import typer
from icecream import ic
from rich.console import Console
from rich.markdown import Markdown
from tqdm import tqdm, trange

ic.configureOutput(prefix="ic debug| -> ")
ic.enable()

app = typer.Typer()

DREMIO_ORIGIN = "http://localhost:9047"


def handle_dremio_error(resp):
    if resp.status_code != 200:
        ic(f"Call failed with {resp.status_code}")

        try:

            err = resp.json()
            ic(f"Error Message: {err['errorMessage']}")
            ic(f"Details: {err.get('moreInfo','')}")
        except json.decoder.JSONDecodeError:
            ic(f"Error in fetching error details...")

        ic("Terminating...")
        sys.exit(1)


def auth():
    url = f"{DREMIO_ORIGIN}/apiv2/login"
    ic(url)
    username, password = os.environ["DREMIO_USER"], os.environ["DREMIO_PASS"]

    ic(f"ask for token with {username}/{password}")
    resp = req.post(url, json={"userName": username, "password": password})

    handle_dremio_error(resp)

    return resp.json()


@app.command()
def authenticate():
    r = auth()
    token = r["token"]
    ic(f"Using {token} as the token")

    print(token)


@app.command()
def catalog(token: str, path: Union[str, None] = None):
    ic("Getting catalog")

    if path is None:
        url = f"{DREMIO_ORIGIN}/api/v3/catalog"
    else:
        url = f"{DREMIO_ORIGIN}/api/v3/catalog/by-path/{path}"
    ic(url)

    resp = req.get(url, headers={"Authorization": token})

    handle_dremio_error(resp)

    if path is None:
        df = pd.DataFrame.from_records(resp.json()["data"])
    else:
        df = pd.DataFrame.from_records(resp.json()["children"])

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_colwidth",
        None,
        "display.max_columns",
        None,
        "display.width",
        300,
    ):  # more options can be specified also
        print(df)


@app.command()
def tag(token: str, id: str):
    ic("Getting Tags")

    url = f"{DREMIO_ORIGIN}/api/v3/catalog/{id}/collaboration/tag"
    ic(url)

    resp = req.get(
        url, headers={"Authorization": token, "Content-Type": "application/json"}
    )

    handle_dremio_error(resp)

    ic(resp.json())

    df = pd.DataFrame(dict(tags=resp.json()["tags"]))

    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 200
    ):  # more options can be specified also
        print(df)


@app.command()
def wiki(token: str, id: str):
    ic("Getting Wiki")

    url = f"{DREMIO_ORIGIN}/api/v3/catalog/{id}/collaboration/wiki"
    ic(url)

    resp = req.get(
        url, headers={"Authorization": token, "Content-Type": "application/json"}
    )

    handle_dremio_error(resp)

    console = Console()
    console.print(Markdown(resp.json()["text"]))


@app.command()
def graph(token: str, id: str):
    """Enterprise edition only"""

    url = f"{DREMIO_ORIGIN}/api/v3/catalog/{id}/graph"
    ic(url)

    resp = req.get(
        url, headers={"Authorization": token, "Content-Type": "application/json"}
    )

    handle_dremio_error(resp)

    ic(resp.json())


@app.command()
def run_sql(token: str, sql_file: typer.FileText, limit: int = 100, offset: int = 0, noreturn:bool = True):
    sql = sql_file.read()

    url = f"{DREMIO_ORIGIN}/api/v3/sql"
    ic(url)

    resp = req.post(
        url,
        headers={"Authorization": token, "Content-Type": "application/json"},
        json=dict(sql=sql),
    )

    handle_dremio_error(resp)

    j = resp.json()
    job_id = j["id"]

    ic(f"Use Job#= {job_id} to fetch results or control job completion...")

    if not noreturn:

        url = f"{DREMIO_ORIGIN}/api/v3/job/{job_id}"

        MAX_TICK = 60
        TICK_INTERVAL = 1

        ticker = trange(MAX_TICK, desc="Polling...", unit=f"{TICK_INTERVAL} sec poll")

        for _ in ticker:
            resp = req.get(
                url,
                headers={"Authorization": token, "Content-Type": "application/json"},
                timeout=10,
            )

            handle_dremio_error(resp)

            j = resp.json()

            if j["jobState"] == "FAILED":
                ic(j)
                ticker.update(MAX_TICK)
                break

            if j["jobState"] == "COMPLETED":
                ic(j)
                print(f"Row count= {j['rowCount']}")
                ticker.update(MAX_TICK)
                break

            time.sleep(TICK_INTERVAL)

        

        o = offset
        l = j['rowCount']

        all_rows = []

        rowcount = tqdm(total=j['rowCount'],desc="rows fetched",unit="row")

        while l > 0:

            url = f"{DREMIO_ORIGIN}/api/v3/job/{job_id}/results?offset={o}&limit=500"

            resp = req.get(
                url, headers={"Authorization": token, "Content-Type": "application/json"}
            )

            all_rows += resp.json()["rows"]

            rowcount.update(len(all_rows))

            o += 500
            l -= 500
        rowcount.update(len(all_rows))


        df = pd.DataFrame.from_records(all_rows)

        ic(f"Total rows {len(df)}")

        with pd.option_context(
            "display.max_rows", None, "display.max_columns", None, "display.width", 200
        ):  # more options can be specified also
            print(df.head())


if __name__ == "__main__":
    app()
