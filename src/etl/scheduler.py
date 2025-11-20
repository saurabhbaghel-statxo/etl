import os
import uuid
import time
import logging
import functools
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional, Literal, DefaultDict, Generator, Any, Dict
from collections import defaultdict
from enum import Enum

import pandas as pd

from ._utils import generate_random_id

logger = logging.getLogger(__name__)

class Schedule:
    HOURLY=1
    '''Repeat job every hour'''

    DAILY=24
    '''Repeats every Day'''

    WEEKLY=24*7
    '''Repeats every week'''

    N_HOURS=-1
    '''Repeats after evey given number of hours'''

    NONE=0
    '''Does not repeat at all, runs in current time, once.'''





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


# class Scheduler:
#     """
#     Runs tasks at fixed intervals and maintains a persistent log (.prev-jobs.csv).
#     Supports daily, hourly, and custom day/hour intervals.
#     """

#     def __init__(
#             self, 
#             days: Optional[int] = None, 
#             hours: Optional[int] = None,
#             minutes: Optional[int] = None,
#             metadata: Optional[Dict] = None
#     ):
#         """
#         _summary_

#         Parameters
#         ----------
#         days : Optional[int], optional
#             _description_, by default None
#         hours : Optional[int], optional
#             _description_, by default None
#         minutes: Optional[int], optional
#             Minutes, by default None
#         metadata: Optional[Dict], optional
#             Metadata to use in scheduling

#         Usage
#         -----
#         ```
#         scheduler = Scheduler(days=1, hours=2)

#         @scheduler.after_days
#         def cleanup_logs():
#             print("Cleaning logs...")

#         @scheduler.after_hours
#         def backup_database():
#             ...
#             return "Backup complete!"
#         ```
#         """
#         self._metadata = metadata
#         self._repeat_after_seconds_total = 0
#         self._job_id = uuid.uuid4().hex[:10]

#         self._repeat_after_days = days
#         self._repeat_after_hours = hours
#         self._repeat_after_minutes = minutes

#         if days:
#             self._repeat_after_seconds_total = timedelta(days=days).total_seconds()
#         elif hours:
#             self._repeat_after_seconds_total = timedelta(hours=hours).total_seconds()
#         elif minutes: 
#             self._repeat_after_seconds_total = timedelta(minutes=minutes).total_seconds()

#         self.init_helper()

#     def init_helper(self):
#         """Initialize scheduler directories and job table."""
#         self._root_dir = os.getcwd()
#         self._prev_jobs_csv_file = os.path.join(self._root_dir, ".prev-jobs.csv")

#         self._prev_jobs_csv_exists = os.path.exists(self._prev_jobs_csv_file)
#         self._prev_jobs_table = (
#             pd.read_csv(self._prev_jobs_csv_file)
#             if self._prev_jobs_csv_exists
#             else pd.DataFrame(
#                 columns=[
#                     "job_id",
#                     "job_name",
#                     "schedule",
#                     "first_ran",
#                     "last_run",
#                     "time_taken",
#                     "metadata",
#                 ]
#             )
#         )

#         self._data = JobMetaData(metadata=self._metadata)

#     # --- CORE LOGGING ----
#     def _update_prev_jobs(self, job_name: str, start: float, end: float, schedule: str):
#         """Append job run info to .prev-jobs.csv"""
#         time_taken = round(end - start, 2)
#         now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#         if not self._data.first_ran:
#             self._data.first_ran = now
#         self._data.last_run = now
#         self._data.schedule = schedule

#         new_entry = {
#             "job_id": self._job_id,
#             "job_name": job_name,
#             "schedule": schedule,
#             "first_ran": self._data.first_ran,
#             "last_run": self._data.last_run,
#             "time_taken": time_taken,
#             "metadata": self._data.metadata,
#         }

#         self._prev_jobs_table = pd.concat(
#             [self._prev_jobs_table, pd.DataFrame([new_entry])], ignore_index=True
#         )
#         self._prev_jobs_table.to_csv(self._prev_jobs_csv_file, index=False)

#         logger.info("Updated job record for %s (time taken = %.2fs)", job_name, time_taken)

#     # --- CORE RUNNER ---
#     def _run_forever(self, func, schedule_name: str, interval_seconds: float, *args, **kwargs):
#         """Generic infinite loop for job execution."""
#         logger.info("Starting job `%s` (%s)", func.__name__, schedule_name)
#         while True:
#             start = time.time()
#             yield func(*args, **kwargs)
#             end = time.time()
#             self._update_prev_jobs(func.__name__, start, end, schedule_name)
#             time.sleep(interval_seconds)

#     # --- SCHEDULERS ---
#     def after_days(self, func: Callable[..., Any]):
#         """Decorator — run a function every N days."""
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs) -> Generator[Any, None, None]:
#             return self._run_forever(
#                 func, "n_days", timedelta(days=self._repeat_after_days).total_seconds(), *args, **kwargs
#             )
#         return wrapper

#     def after_hours(self, func: Callable[..., Any]):
#         """Decorator — run a function every N hours."""
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs) -> Generator[Any, None, None]:
#             return self._run_forever(
#                 func, "n_hours", timedelta(hours=self._repeat_after_hours).total_seconds(), *args, **kwargs
#             )
#         return wrapper

#     def after_minutes(self, func: Callable[..., Any]):
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs) -> Generator[Any, None, None]:
#             return self._run_forever(
#                 func, "n_hours", timedelta(minutes=self._repeat_after_minutes).total_seconds(), *args, **kwargs
#             )
#         return wrapper        

#     def daily(self, func: Callable[..., Any], at: str = "00:00"):
#         """
#         Decorator — run a function once per calendar day, at a fixed time (HH:MM).
#         Example: @scheduler.daily(at="03:30")
#         """
#         hh, mm = map(int, at.split(":"))

#         @functools.wraps(func)
#         def wrapper(*args, **kwargs) -> Generator[Any, None, None]:
#             logger.info("Scheduling daily job `%s` at %02d:%02d", func.__name__, hh, mm)
#             while True:
#                 now = datetime.now()
#                 run_time = now.replace(hour=hh, minute=mm, second=0, microsecond=0)

#                 # If today's run time has passed, schedule for tomorrow
#                 if run_time <= now:
#                     run_time += timedelta(days=1)

#                 wait_seconds = (run_time - now).total_seconds()
#                 logger.info("Next run for `%s` in %.1f seconds", func.__name__, wait_seconds)

#                 time.sleep(wait_seconds)
#                 start = time.time()
#                 yield func(*args, **kwargs)
#                 end = time.time()

#                 self._update_prev_jobs(func.__name__, start, end, "daily")

#         return wrapper
    

# class Scheduler:
#     """
#     Scheduler for running functions repeatedly at a specified interval.
#     Supports both synchronous and asynchronous functions.
#     """
#     def __init__(self, time_delta_in_seconds: int = 0):
#         """
#         Parameters
#         ----------
#         time_delta_in_seconds : int, optional
#             Interval between executions in seconds, by default 0

#         Usage
#         -----
#         ```python
#         # Sync
#         scheduler = Scheduler(time_delta_in_seconds=5)
#         for result in scheduler.run(my_function, arg1):
#             print(result)

#         # Async
#         async_scheduler = Scheduler(time_delta_in_seconds=5)
#         async for result in async_scheduler.arun(my_async_function, arg1):
#             print(result)
#         ```
#         """
#         self._time_delta_in_seconds = time_delta_in_seconds
        
#     # --- CORE RUNNERS ---
#     def _run_forever(self, func, schedule_name: str, interval_seconds: float, x: Optional[Any], *args, **kwargs):
#         """Generic infinite loop for job execution (sync version, yields results)."""
#         logger.info("Starting job `%s` (%s)", func.__name__, schedule_name)
#         while True:
#             start = time.time()
#             result = func(x, *args, **kwargs)
#             elapsed = time.time() - start
#             yield result
#             # Sleep for remaining time after execution
#             sleep_time = max(0, interval_seconds - elapsed)
#             time.sleep(sleep_time)

#     async def _arun_forever(self, func, schedule_name: str, interval_seconds: float, *args, **kwargs):
#         """Generic infinite loop for job execution (async version, yields results)."""
#         logger.info("Starting job `%s` (%s)", func.__name__, schedule_name)
#         while True:
#             start = time.time()
#             result = await func(*args, **kwargs)
#             elapsed = time.time() - start
#             yield result
#             # Sleep for remaining time after execution
#             sleep_time = max(0, interval_seconds - elapsed)
#             await asyncio.sleep(sleep_time)

#     def run(self, func, *args, **kwargs):
#         """
#         Run a synchronous function repeatedly.
        
#         Returns a generator that yields results from each execution.
        
#         Usage:
#         ------
#         ```python
#             scheduler = Scheduler(time_delta_in_seconds=5)
#             for result in scheduler.run(my_function, arg1, arg2):
#                 print(result)
#         ```
#         """
#         schedule_name = f"Scheduler_{self._time_delta_in_seconds}s_{generate_random_id()}"
#         return self._run_forever(
#             func, 
#             schedule_name=schedule_name,
#             interval_seconds=self._time_delta_in_seconds,
#             *args, **kwargs
#         )

#     async def arun(self, func, *args, **kwargs):
#         """
#         Run an asynchronous function repeatedly.
        
#         Returns an async generator that yields results from each execution.
        
#         Usage:
#         ------
#         ```python
#             scheduler = Scheduler(time_delta_in_seconds=5)
#             async for result in scheduler.arun(my_async_function, arg1, arg2):
#                 print(result)
#         ```
        
#         Raises
#         ------
#         RuntimeError
#             If func is not an async function (coroutine function)
#         """
#         if not asyncio.iscoroutinefunction(func):
#             logger.error("Function is not a coroutine function: %s", func.__name__)
#             raise RuntimeError(f"{func.__name__} is not an async function")
        
#         schedule_name = f"Scheduler_{self._time_delta_in_seconds}s_{generate_random_id()}"
#         async for result in self._arun_forever(
#             func, 
#             schedule_name=schedule_name,
#             interval_seconds=self._time_delta_in_seconds,
#             *args, **kwargs
#         ):
#             yield result



class Scheduler:
    """
    Scheduler for running functions repeatedly at a specified interval.
    Supports both synchronous and asynchronous functions.
    """
    def __init__(self, repeat_after_seconds: int = 0, callback: Optional[Callable] = None):
        """
        Parameters
        ----------
        repeat_after_seconds : int, optional
            Interval between executions in seconds, by default 0

        Usage
        -----
        ```python
        # Sync
        scheduler = Scheduler(time_delta_in_seconds=5)
        for result in scheduler.run(my_function, arg1):
            print(result)

        # Async
        async_scheduler = Scheduler(time_delta_in_seconds=5)
        async for result in async_scheduler.arun(my_async_function, arg1):
            print(result)
        ```
        """
        self._repeat_after_seconds = repeat_after_seconds
        self._callback = callback
        self._running_tasks = list()    # to store the tasks running

    def add_task(self, task: asyncio.Task):
        self._running_tasks.append(task)

    def remove_task(self, task: asyncio.Task):
        self._running_tasks.remove(task)
        # NOTE: this removes the first instance, whichever it is

    async def stop_all(self):
        for t in self._running_tasks:
            t.cancel()  # handle this when t is just a callable
        self._running_tasks.clear()

        
    # --- CORE RUNNERS ---
    def _run_forever(self, executable, x: Optional[Any], schedule_name: str, interval_seconds: float, *args, **kwargs):
        """Generic infinite loop for job execution (sync version, yields results)."""
        logger.info("Starting job `%s` (%s)", executable.__name__, schedule_name)
        while True:
            start = time.time()
            result = executable.run(x, *args, **kwargs)
            elapsed = time.time() - start
            yield result
            # Sleep for remaining time after execution
            sleep_time = max(0, interval_seconds - elapsed)
            time.sleep(sleep_time)

    async def _arun_forever(self, executable, x: Optional[Any], schedule_name: str, interval_seconds: float, *args, **kwargs):
        """Generic infinite loop for job execution (async version, yields results)."""
        from etl.executable import _ExecutableMeta

        logger.info("Starting job `%s` (%s)", executable.__name__, schedule_name)

        if hasattr(executable, 'arun') and callable(getattr(executable, 'arun')):   # ducktype checking
        
            while True:
                start = time.time()
                result = await executable.arun(x, *args, **kwargs)
                elapsed = time.time() - start
                yield result
                # Sleep for remaining time after execution
                sleep_time = max(0, interval_seconds - elapsed)
                logger.debug("Sleeping for %d s", sleep_time)
                await asyncio.sleep(sleep_time)

        elif callable(executable):
            logger.debug("Executable %s is a function", executable.__name__)
            while True:
                start = time.time()
                result = await executable(x, *args, **kwargs)
                elapsed = time.time() - start
                yield result
                # Sleep for remaining time after execution
                sleep_time = max(0, interval_seconds - elapsed)
                await asyncio.sleep(sleep_time)

    def run(self, executable, x, *args, **kwargs):
        """
        Run a synchronous function repeatedly.
        
        Returns a generator that yields results from each execution.
        
        Usage:
        ------
        ```python
            scheduler = Scheduler(time_delta_in_seconds=5)
            for result in scheduler.run(my_function, arg1, arg2):
                print(result)
        ```
        """
        schedule_name = f"Scheduler_{self._repeat_after_seconds}s_{generate_random_id()}"
        return self._run_forever(
            executable, 
            x, 
            schedule_name=schedule_name,
            interval_seconds=self._repeat_after_seconds,
            *args, **kwargs
        )

    async def arun(self, executable, x, *args, **kwargs):
        """
        Run an asynchronous function repeatedly.
        
        Returns an async generator that yields results from each execution.
        
        Usage:
        ------
        ```python
            scheduler = Scheduler(time_delta_in_seconds=5)
            async for result in scheduler.arun(my_async_function, arg1, arg2):
                print(result)
        ```
        
        Raises
        ------
        RuntimeError
            If func is not an async function (coroutine function)
        """

        schedule_name = f"Scheduler_{self._repeat_after_seconds}s_{generate_random_id()}"
        async for res in self._arun_forever(
            executable, 
            x,
            schedule_name=schedule_name,
            interval_seconds=self._repeat_after_seconds,
            *args, **kwargs
        ):
            if self._callback:
                if asyncio.iscoroutinefunction(self._callback):
                    await self._callback(res)
                else:
                    self._callback(res)
            yield res

    def schedule(self, executable: Callable[..., Any]):   
        def _wrapper(*args, **kwargs):
            return self._run_forever(
                executable, 
                f"scheduler_{executable.__name__}", 
                self._repeat_after_seconds,
                *args, **kwargs 
            )
        return _wrapper
    