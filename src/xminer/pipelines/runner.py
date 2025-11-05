# src/xminer/pipelines/runner.py
from __future__ import annotations
import logging
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

class Step:
    def __init__(self, name: str, fn: Callable, kwargs: dict | None = None):
        self.name = name
        self.fn = fn
        self.kwargs = kwargs or {}

    def run(self):
        logger.info("▶️  Step: %s", self.name)
        return self.fn(**self.kwargs)

class Pipeline:
    def __init__(self, name: str, steps: Iterable[Step]):
        self.name = name
        self.steps = list(steps)

    def run(self):
        logger.info("🚀 Pipeline: %s (steps=%d)", self.name, len(self.steps))
        for s in self.steps:
            s.run()
        logger.info("✅ Pipeline finished: %s", self.name)
