from __future__ import annotations

import importlib
import pkgutil
from typing import Dict

from . import base

REGISTRY: Dict[str, base.Service] = {}


def register(service: base.Service) -> None:
    REGISTRY[service.NODE_KIND] = service


def load_plugins() -> None:
    """Auto-discover managed services in this package."""
    for m in pkgutil.iter_modules(__path__):  # type: ignore[name-defined]
        if m.name in ("base", "__init__"):
            continue
        mod = importlib.import_module(f"{__name__}.{m.name}")
        if hasattr(mod, "SERVICE"):
            register(mod.SERVICE)
