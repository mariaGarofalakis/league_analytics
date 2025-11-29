from src.utils.logger import get_logger
from src.validators import expectations as exp
from typing import Tuple

logger = get_logger(__name__)


class ExpectationRunner:
    def __init__(self, mode: str = "soft"):
        assert mode in ["soft", "strict"], "Mode must be 'soft' or 'strict'"
        self.mode = mode
        self.errors = []

    def run(self, result: Tuple[bool, str], label: str = ""):
        success, message = result
        if success:
            logger.info(f"✅ {label or message}")
        else:
            full_msg = f"❌ {label or message}"
            if self.mode == "strict":
                raise AssertionError(full_msg)
            else:
                logger.warning(full_msg)
                self.errors.append((label, message))

    def has_errors(self) -> bool:
        return len(self.errors) > 0
    
    def report(self):
        if self.errors:
            logger.warning("🔍 Validation issues:")
            for label, message in self.errors:
                logger.warning(f"• {label} ➜ {message}")
        else:
            logger.info("✅ All validations passed with no issues.")
