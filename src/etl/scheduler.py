import os
import uuid
import time
import logging
import functools
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional, Literal, DefaultDict, Generator, Any
from collections import defaultdict

logger = logging.getLogger(__name__)

@dataclass
class JobMetaData:
    _id: str = field(default_factory=lambda: uuid.uuid4().hex[:8], init=False)
    """Unique id of the job"""

    schedule: Optional[Literal[
        "hourly",
        "daily",
        "weekly",
        "n_days",
        "n_hours"
    ]] = None
    """Schedule type"""

    first_ran: Optional[str] = None
    """Datetime when job first ran"""

    last_run: Optional[str] = None
    """Datetime when job last ran"""

    metadata: DefaultDict[str, Any] = field(default_factory=lambda: defaultdict(dict))
    """Additional metadata"""


class Scheduler:
    """
    Runs tasks at fixed intervals and maintains a persistent log (.prev-jobs.csv).
    Supports daily, hourly, and custom day/hour intervals.
    """

    def __init__(self, days: Optional[int] = None, hours: Optional[int] = None):
        """
        _summary_

        Parameters
        ----------
        days : Optional[int], optional
            _description_, by default None
        hours : Optional[int], optional
            _description_, by default None

        Usage
        -----
        ```
        scheduler = Scheduler(days=1, hours=2)

        @scheduler.after_days
        def cleanup_logs():
            print("Cleaning logs...")

        @scheduler.after_hours
        def backup_database():
            ...
            return "Backup complete!"
        ```
        """
        self._repeat_after_seconds_total = 0
        self._job_id = uuid.uuid4().hex[:10]

        self._repeat_after_days = days
        self._repeat_after_hours = hours

        if days:
            self._repeat_after_seconds_total = timedelta(days=days).total_seconds()
        elif hours:
            self._repeat_after_seconds_total = timedelta(hours=hours).total_seconds()

        self.init_helper()

    def init_helper(self):
        """Initialize scheduler directories and job table."""
        self._root_dir = os.getcwd()
        self._prev_jobs_csv_file = os.path.join(self._root_dir, ".prev-jobs.csv")

        self._prev_jobs_csv_exists = os.path.exists(self._prev_jobs_csv_file)
        self._prev_jobs_table = (
            pd.read_csv(self._prev_jobs_csv_file)
            if self._prev_jobs_csv_exists
            else pd.DataFrame(
                columns=[
                    "job_id",
                    "job_name",
                    "schedule",
                    "first_ran",
                    "last_run",
                    "time_taken",
                    "metadata",
                ]
            )
        )

        self._data = JobMetaData()

    # --- CORE LOGGING ----
    def _update_prev_jobs(self, job_name: str, start: float, end: float, schedule: str):
        """Append job run info to .prev-jobs.csv"""
        time_taken = round(end - start, 2)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not self._data.first_ran:
            self._data.first_ran = now
        self._data.last_run = now
        self._data.schedule = schedule

        new_entry = {
            "job_id": self._job_id,
            "job_name": job_name,
            "schedule": schedule,
            "first_ran": self._data.first_ran,
            "last_run": self._data.last_run,
            "time_taken": time_taken,
            "metadata": dict(self._data.metadata),
        }

        self._prev_jobs_table = pd.concat(
            [self._prev_jobs_table, pd.DataFrame([new_entry])], ignore_index=True
        )
        self._prev_jobs_table.to_csv(self._prev_jobs_csv_file, index=False)

        logger.info("Updated job record for %s (time taken = %.2fs)", job_name, time_taken)

    # --- CORE RUNNER ---
    def _run_forever(self, func, schedule_name: str, interval_seconds: float, *args, **kwargs):
        """Generic infinite loop for job execution."""
        logger.info("Starting job `%s` (%s)", func.__name__, schedule_name)
        while True:
            start = time.time()
            yield func(*args, **kwargs)
            end = time.time()
            self._update_prev_jobs(func.__name__, start, end, schedule_name)
            time.sleep(interval_seconds)

    # --- SCHEDULERS ---
    def after_days(self, func: Callable[..., Any]):
        """Decorator — run a function every N days."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Generator[Any, None, None]:
            return self._run_forever(
                func, "n_days", timedelta(days=self._repeat_after_days).total_seconds(), *args, **kwargs
            )
        return wrapper

    def after_hours(self, func: Callable[..., Any]):
        """Decorator — run a function every N hours."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Generator[Any, None, None]:
            return self._run_forever(
                func, "n_hours", timedelta(hours=self._repeat_after_hours).total_seconds(), *args, **kwargs
            )
        return wrapper

    def daily(self, func: Callable[..., Any], at: str = "00:00"):
        """
        Decorator — run a function once per calendar day, at a fixed time (HH:MM).
        Example: @scheduler.daily(at="03:30")
        """
        hh, mm = map(int, at.split(":"))

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Generator[Any, None, None]:
            logger.info("Scheduling daily job `%s` at %02d:%02d", func.__name__, hh, mm)
            while True:
                now = datetime.now()
                run_time = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

                # If today's run time has passed, schedule for tomorrow
                if run_time <= now:
                    run_time += timedelta(days=1)

                wait_seconds = (run_time - now).total_seconds()
                logger.info("Next run for `%s` in %.1f seconds", func.__name__, wait_seconds)

                time.sleep(wait_seconds)
                start = time.time()
                yield func(*args, **kwargs)
                end = time.time()

                self._update_prev_jobs(func.__name__, start, end, "daily")

        return wrapper
