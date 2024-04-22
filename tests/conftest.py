import os
import sys
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

# Add repo root path for imports
repo_root = Path(__file__).parent.parent
sys.path.append(str(repo_root.absolute()))

# Dev note:
# test logic assumes the webbook env vars are
# not set. So unset them for the length of
# tests in the event they are currently set.
mp = MonkeyPatch()
mp.setenv("DISABLE_NOTIFICATIONS", "True")
for potential_env_var_name in [
    "PS_SLACK_WEBHOOK",
    "DS_SLACK_WEBHOOK",
    "SE_SLACK_WEBHOOK",
    "DE_SLACK_WEBHOOK",
]:

    env_var = os.environ.get(potential_env_var_name, None)
    if env_var is not None:
        mp.delenv(potential_env_var_name)
