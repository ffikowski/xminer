# src/xminer/pipelines/cli.py
from __future__ import annotations
import logging
import typer
from .flows import pipeline_fetch, pipeline_metrics, pipeline_all

app = typer.Typer(add_completion=False)

def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

@app.command()
def run(name: str = typer.Argument(..., help="fetch | metrics | all")):
    _setup_logging()
    name = name.lower()
    if name == "fetch":
        p = pipeline_fetch()
    elif name == "metrics":
        p = pipeline_metrics()
    elif name == "all":
        p = pipeline_all()
    else:
        raise typer.BadParameter("Unknown pipeline. Use: fetch, metrics, all")
    p.run()

if __name__ == "__main__":
    app()
