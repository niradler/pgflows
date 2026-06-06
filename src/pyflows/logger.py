from __future__ import annotations

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a structured logger for the given pyflows component name.

    All pyflows loggers share the 'pyflows' namespace and inherit from the
    root 'pyflows' logger so users can configure them in one place:

        import logging
        logging.getLogger("pyflows").setLevel(logging.DEBUG)
    """
    return logging.getLogger(f"pyflows.{name}")


def configure_default_logging(level: int = logging.INFO) -> None:
    """Install a sensible default handler on the root pyflows logger.

    Call once at app startup if you don't configure logging yourself.
    """
    logger = logging.getLogger("pyflows")
    if logger.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.setLevel(level)
