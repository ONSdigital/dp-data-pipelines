import subprocess

from dpypelines.pipeline.shared.utils import get_commit_id


def test_get_commit_id():
    git_cli_commit_hash = (
        subprocess.check_output(["git", "log", "-1", "--format=%H"])
        .strip()
        .decode("utf-8")
    )
    utils_commit_hash = get_commit_id()
    assert git_cli_commit_hash == utils_commit_hash
