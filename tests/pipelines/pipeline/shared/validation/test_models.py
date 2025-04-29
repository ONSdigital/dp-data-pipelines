import pytest
from dpypelines.pipeline.validation.models import ValidationResult

test_data = [
    (True, "csv", None),
    (True, "other-format", None),
    (True, "excel", "Should't be an error here but doesn't matter"),
    (False, "json", "Some error here"),
    (False, "xls", ""),
]


@pytest.mark.parametrize("success,format,error", test_data)
def test_should_write_success_to_string(success: bool, format: str, error):
    result = ValidationResult(success, format, error)

    result_string = result.__repr__()

    assert "[ValidationResult]" in result_string, result_string
    assert str(success) in result_string, result_string
    assert str(not success) not in result_string, result_string
    assert format in result_string, result_string
    if error is None:
        assert "error: None" in result_string
    else:
        assert f"'{error}'" in result_string, result_string
