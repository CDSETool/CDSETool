"""
File download monitoring.

This module provides a StatusMonitor class that can be used to monitor the
progress of file downloads.

For non-interactive programs the NoopMonitor class can be used to disable
status monitoring.
"""

import time
import threading
import sys
import os
import signal
import shutil

IS_IPYTHON = True

try:
    from IPython import get_ipython
    from IPython.display import clear_output

    if "IPKernelApp" not in get_ipython().config:
        IS_IPYTHON = False
except ImportError:
    IS_IPYTHON = False
except AttributeError:
    IS_IPYTHON = False


class StatusMonitor(threading.Thread):
    """
    A monitor that prints a status bar for each download

    Usage:

    with StatusMonitor() as monitor:
        with monitor.status() as status:
            status.set_filename("filename.txt")
            status.set_filesize(1024)
            status.add_progress(512)

            time.sleep(10)

            status.add_progress(512)
    """

    line_length = 80

    __is_running = True
    __progress_lines = 0

    __download_speed_deltas = []
    __done = []
    __status = []

    def start(self):
        """
        Start the monitor
        """

        def _set_line_length(_signal_num, _stack):
            self.line_length, _ = shutil.get_terminal_size()

        _set_line_length(None, None)

        if os.name != "nt":
            signal.signal(signal.SIGWINCH, _set_line_length)

        super().start()

    def stop(self):
        """
        Stop the monitor
        """
        self.__is_running = False

    def status(self):
        """
        Returns a status bar for a single download
        """
        status = Status(self)
        self.__status.append(status)
        return status

    def remove_status(self, status):
        """
        Remove a status from the monitor, marking it as done
        """
        self.__done.append(status)
        self.__status.remove(status)

    def run(self):
        """
        Main loop for the monitor, printing the status bars every second until stopped
        """
        while True:
            self.__track_download_speed()
            if self.__is_running is False:
                break

            self.__clear_progress_lines()
            self.__print_done_lines()
            self.__draw()

        print("")

    @property
    def __download_speed(self):
        if len(self.__download_speed_deltas) < 2:
            return 0
        return sum(self.__download_speed_deltas) / len(self.__download_speed_deltas)

    def __track_download_speed(self):
        speed_t0 = self.__total_downloaded
        time.sleep(1)
        speed_t1 = self.__total_downloaded
        self.__download_speed_deltas.append(speed_t1 - speed_t0)
        if len(self.__download_speed_deltas) > 10:
            self.__download_speed_deltas.pop(0)

    def __print_done_lines(self):
        for status in self.__done:
            print(status.done_line())

    def __clear_progress_lines(self):
        if IS_IPYTHON:
            clear_output(wait=True)
            return

        sys.stdout.write("\033[K")
        for _ in range(self.__progress_lines + 2):
            sys.stdout.write("\033[F\033[K")

        for _ in self.__done:
            sys.stdout.write("\033[F\033[K")

        print("")
        print("")

    def __draw(self):
        self.__progress_lines = 1

        print(
            " | ".join(
                [
                    "[[ ",
                    f"{len(self.__status)} files in progress",
                    f"{len(self.__done)} files done",
                    f"{bytes_to_human(self.__total_downloaded)} total downloaded",
                    f"{bytes_to_human(self.__download_speed)}/s ]]",
                ]
            )
        )

        for status in self.__status:
            filename_line, progress_line = status.status_lines()
            print(filename_line.ljust(self.line_length, " "))
            print(progress_line.ljust(self.line_length, " "))
            self.__progress_lines += 2

    @property
    def __total_downloaded(self):
        return sum(status.downloaded for status in self.__status) + sum(
            status.size for status in self.__done
        )

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class NoopMonitor:
    """
    A monitor that does nothing
    """

    def status(self):
        """
        Returns a status bar for a single download
        """
        return Status(self)

    def remove_status(self, status):
        """
        Remove a status from the monitor
        """

    def start(self):
        """
        Start the monitor
        """

    def stop(self):
        """
        Stop the monitor
        """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class Status:
    """
    A status bar for a single download
    """

    __monitor = None
    filename = None
    size = 0
    downloaded = 0

    def done_line(self):
        """
        Returns a line to print when the download is complete
        """
        if self.size == 0 and self.downloaded == 0:
            return f"{self.filename} skipped"
        return f"{self.filename} ({bytes_to_human(self.size)})"

    def status_lines(self):
        """
        Returns a tuple of lines to print for the status bar
        """
        line_length = self.__monitor.line_length
        if self.downloaded == 0:
            return (
                "Thread waiting for connection to start...",
                f"[{' ' * (line_length - 2)}]",
            )

        progress = self.downloaded / self.size
        filename_line = (
            f"{self.filename[0:line_length - 6]} "
            + f"{bytes_to_human(self.size)} ({int(progress * 100)}%)"
        )
        progress_line = (
            "["
            + f"{'█' * int(progress * (line_length - 2))}"
            + f"{' ' * (line_length - int(progress * (line_length - 2)) - 2)}"
            + "]"
        )

        return filename_line, progress_line

    def add_progress(self, chunk_bytes):
        """
        Add to the number of bytes downloaded
        """
        self.downloaded += chunk_bytes

    def set_filename(self, filename):
        """
        Set the name of the file being downloaded
        """
        self.filename = filename

    def set_filesize(self, size):
        """
        Set the size of the file being downloaded
        """
        self.size = size

    def __init__(self, monitor):
        self.__monitor = monitor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__monitor.remove_status(self)


def bytes_to_human(num_bytes):
    """
    Convert a number of bytes to a human-readable string
    """
    if num_bytes < 1000:
        return f"{num_bytes} B"
    if num_bytes < 1000000:
        return f"{num_bytes / 1000:.2f} KB"
    if num_bytes < 1000000000:
        return f"{num_bytes / 1000000:.2f} MB"
    if num_bytes < 1000000000000:
        return f"{num_bytes / 1000000000:.2f} GB"

    return f"{num_bytes / 1000000000000:.2f} TB"
