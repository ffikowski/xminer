# src/xminer/pipelines/flows.py
from __future__ import annotations
import logging
from ..config.params import Params
from ..tasks import (
    fetch_x_profiles as T_fetch_x_profiles,
    fetch_tweets as T_fetch_tweets,
    x_profile_metrics_monthly as T_prof_month,
    x_profile_metrics_delta as T_prof_delta,
    tweets_metrics_monthly as T_tweets_month,
)
from .runner import Pipeline, Step

logger = logging.getLogger(__name__)

def _common():
    year  = int(getattr(Params, "year"))
    month = int(getattr(Params, "month"))
    outdir = getattr(Params, "outdir", "output")
    top_n = int(getattr(Params, "top_n", 50))
    schema = "public"
    return year, month, outdir, top_n, schema

def pipeline_fetch() -> Pipeline:
    # uses tasks with main()
    steps = [
        Step("fetch_x_profiles", T_fetch_x_profiles.main),
        Step("fetch_tweets",     T_fetch_tweets.main),
    ]
    return Pipeline("fetch", steps)

def pipeline_metrics() -> Pipeline:
    year, month, outdir, top_n, schema = _common()
    steps = [
        Step("x_profile_metrics_monthly",
             T_prof_month.run,
             dict(year=year, month=month, outdir=outdir,
                  schema=schema, x_profiles="x_profiles", top_n=top_n)),
        Step("x_profile_metrics_delta",
             T_prof_delta.run,
             dict(year=year, month=month, outdir=outdir,
                  schema=schema, x_profiles="x_profiles", politicians="politicians", top_n=top_n)),
        Step("tweets_metrics_monthly",
             T_tweets_month.run,
             dict(year=year, month=month, outdir=outdir,
                  schema=schema, tweets_tbl="tweets", x_profiles_tbl="x_profiles", top_n=top_n)),
        # Optional: add tweets_metrics_delta here once implemented
    ]
    return Pipeline("metrics", steps)

def pipeline_all() -> Pipeline:
    # fetch -> metrics
    f = pipeline_fetch().steps
    m = pipeline_metrics().steps
    return Pipeline("all", [*f, *m])
