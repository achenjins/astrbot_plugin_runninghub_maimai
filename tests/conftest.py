"""Keep AstrBot's import-time runtime files away from a real bot installation."""
import os
import tempfile

# Must happen before test modules import astrbot.core.
os.environ["ASTRBOT_ROOT"] = tempfile.mkdtemp(prefix="runninghub-tests-")
