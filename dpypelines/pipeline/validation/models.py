from typing import Optional


class ValidationResult:
    def __init__(self, valid: bool, format: str, error: Optional[str] = None):
        self.valid = valid
        self.format = format
        self.error = error

    def __str__(self):
        return f"[ValidationResult] valid:'{self.valid}', format:'{self.format}', error: {self._format_error()}"

    def _format_error(self):
        if self.error is None:
            return "None"

        return f"'{self.error}'"

    def __repr__(self):
        return self.__str__()
