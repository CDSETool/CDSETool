"""Tests for CDSETool's monitor module."""

import subprocess
import sys
import textwrap

from cdsetool.monitor import StatusMonitor


def test_status_monitor_runs_as_a_daemon_thread() -> None:
    """The monitor loop only ends when stop() flips its flag, so it must not be
    able to keep a dying interpreter alive."""
    assert StatusMonitor().daemon is True


def test_interpreter_exits_when_stop_is_never_called() -> None:
    """A download raising before stop() is reached used to leave the process
    hanging instead of surfacing the error."""
    script = textwrap.dedent(
        """
        from cdsetool.monitor import StatusMonitor

        monitor = StatusMonitor()
        monitor.start()
        # Deliberately no stop(): mimics an exception escaping mid-download.
        """
    )

    try:
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired:  # pragma: no cover - the bug being guarded
        raise AssertionError(
            "interpreter did not exit; the monitor thread is keeping it alive"
        ) from None

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
