import time
import threading
import sys
import os
import signal

line_length, _ = os.get_terminal_size()

def set_line_length(_a, _b):
    global line_length
    line_length, _ = os.get_terminal_size()

signal.signal(signal.SIGWINCH, set_line_length)

class StatusMonitor(threading.Thread):
    __is_running = True
    __progress_lines = 0

    __download_speed_deltas = []
    __done = []
    __status = []

    def status(self):
        status = Status(self)
        self.__status.append(status)
        return status

    def remove_status(self, status):
        self.__done.append(status)
        self.__status.remove(status)

    def stop(self):
        self.__is_running = False

    def run(self):
        while True:
            self.__track_download_speed()
            if self.__is_running == False:
                break

            self.__clear_progress_lines()
            self.__print_done_lines()
            self.__draw()

        print("")

    def __download_speed(self):
        if len(self.__download_speed_deltas) < 2:
            return 0
        return sum(self.__download_speed_deltas) / len(self.__download_speed_deltas)

    def __track_download_speed(self):
        a = self.__total_downloaded()
        time.sleep(1)
        b = self.__total_downloaded()
        self.__download_speed_deltas.append(b - a)
        if len(self.__download_speed_deltas) > 10:
            self.__download_speed_deltas.pop(0)

    def __print_done_lines(self):
        for status in self.__done:
            print(status.done_line())

    def __clear_progress_lines(self):
        sys.stdout.write("\033[K")
        for _ in range(self.__progress_lines + 2):
            sys.stdout.write("\033[F\033[K")

        for _ in self.__done:
            sys.stdout.write("\033[F\033[K")

        print("")
        print("")

    def __draw(self):
        self.__progress_lines = 1

        print(f"[[ {len(self.__status)} files in progress | {len(self.__done)} files done | {bytes_to_human(self.__total_downloaded())} total downloaded | {bytes_to_human(self.__download_speed())}/s ]]")

        for status in self.__status:
            filename_line, progress_line = status.status_lines()
            print(filename_line.ljust(line_length, " "))
            print(progress_line.ljust(line_length, " "))
            self.__progress_lines += 2

    def __total_downloaded(self):
        return sum([status.downloaded for status in self.__status]) + sum([status.size for status in self.__done])

    def __enter__(self):
        print("starting monitor")
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

class NoopMonitor:
    def status(self):
        return Status(self)

    def remove_status(self, status):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class Status:
    __monitor = None
    filename = None
    size = 0
    downloaded = 0

    def done_line(self):
        if self.size == 0 and self.downloaded == 0:
            return f"{self.filename} skipped"
        return f"{self.filename} ({bytes_to_human(self.size)})"

    def status_lines(self):
        if self.downloaded == 0:
            return 'Thread waiting for connection to start...', f"[{' ' * (line_length - 2)}]"

        progress = self.downloaded / self.size
        filename_line = f"{self.filename[0:line_length - 6]} {bytes_to_human(self.size)} ({int(progress * 100)}%)"
        progress_line = f"[{'█' * int(progress * (line_length - 2))}{' ' * (line_length - int(progress * (line_length - 2)) - 2)}]"

        return filename_line, progress_line

    def update_progress(self, chunk_bytes):
        self.downloaded += chunk_bytes

    def set_filename(self, filename):
        self.filename = filename

    def set_filesize(self, size):
        self.size = size

    def __init__(self, monitor):
        self.__monitor = monitor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__monitor.remove_status(self)

def bytes_to_human(bytes):
    if bytes < 1000:
        return f"{bytes} B"
    elif bytes < 1000000:
        return f"{bytes / 1000:.2f} KB"
    elif bytes < 1000000000:
        return f"{bytes / 1000000:.2f} MB"
    elif bytes < 1000000000000:
        return f"{bytes / 1000000000:.2f} GB"
    else:
        return f"{bytes / 1000000000000:.2f} TB"
