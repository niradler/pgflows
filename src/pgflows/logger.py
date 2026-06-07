from __future__ import annotations

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a structured logger for the given pgflows component name.

    All pgflows loggers share the 'pgflows' namespace and inherit from the
    root 'pgflows' logger so users can configure them in one place:

        import logging
        logging.getLogger("pgflows").setLevel(logging.DEBUG)
    """
    return logging.getLogger(f"pgflows.{name}")


def configure_default_logging(level: int = logging.INFO) -> None:
    """Install a sensible default handler on the root pgflows logger.

    Call once at app startup if you don't configure logging yourself.
    """
    logger = logging.getLogger("pgflows")
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
