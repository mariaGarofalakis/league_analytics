import logging
import sys
from pathlib import Path


from src.config.settings import get_settings

settings = get_settings()




# -----------------------------
# Unified Logging Setup
# -----------------------------
def configure_global_logging(log_level: int = logging.INFO) -> None:
    """Configure root and library loggers with consistent formatting."""
    APP_NAME = settings.APP_NAME
    log_format = f"[{APP_NAME}] %(asctime)s | %(levelname)-8s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Root logger setup
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.handlers.clear()

    # Console output
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File output
    # Path(settings.LOG_DIR).mkdir(parents=True, exist_ok=True)
    # file_handler = logging.FileHandler(Path(settings.LOG_DIR) / f"{APP_NAME}.log")
    # file_handler.setLevel(log_level)
    # file_handler.setFormatter(formatter)
    # root_logger.addHandler(file_handler)

    # Unify known third-party loggers
    # for lib_logger_name in [
    #     "uvicorn",
    #     "uvicorn.error",
    #     "uvicorn.access",
    #     "transformers",
    #     "datasets",
    #     "torch",
    #     "torchmetrics",
    #     "evaluate",
    #     "fastapi",
    #     "starlette",
    # ]:
    #     lib_logger = logging.getLogger(lib_logger_name)
    #     lib_logger.handlers.clear()
    #     handler = logging.StreamHandler(sys.stdout)
    #     handler.setFormatter(formatter)
    #     lib_logger.addHandler(handler)
    #     lib_logger.setLevel(log_level)
    #     lib_logger.propagate = False

    root_logger.info(
        f"Logging configured for {APP_NAME} (level={logging.getLevelName(log_level)})"
    )


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a named logger after global setup."""
    return logging.getLogger(name or settings.APP_NAME)
