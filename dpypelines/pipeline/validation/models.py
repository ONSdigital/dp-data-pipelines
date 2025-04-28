from typing import Optional


class ValidationResult:
    def __init__(self, valid: bool, format: str, error: Optional[str] = None):
        self.valid = valid
        self.format = format
        self.error = error
