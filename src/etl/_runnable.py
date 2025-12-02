# is the thing which is actually run
# can convert any thing to be run by the runner

from typing import Any, Optional, Literal, Union
import logging
from dataclasses import dataclass, field

from .scheduler import Scheduler, Schedule
from .executable import _ExecutableMeta
from ._utils import are_metaclasses_compatible, get_root_metaclass

logger = logging.getLogger(__name__)

# @dataclass
class Runnable(metaclass=_ExecutableMeta):
    """
    Wrapper for executable operations with scheduling and status tracking capabilities.
    
    Runnable provides a unified interface for wrapping any executable operation (Transform,
    Downloader, Validator, etc.) with additional features like scheduling, thread tracking,
    and status management. It acts as an adapter between executables and the Runner framework,
    allowing any class with a `run()` method to be orchestrated by Runner.
    
    The class uses a factory pattern (`make()` classmethod) to dynamically create subclasses
    that combine Runnable functionality with existing executable classes, enabling seamless
    integration without modifying source code.
    
    Attributes
    ----------
    executable : Optional[Any]
        The underlying executable object to be run by the Runner. Typically an instance
        of Transform, Downloader, Validator, or any class implementing `run()` and `arun()`.
        Set via the `make()` factory method or manually after initialization.
    
    thread_id : Optional[str]
        Unique identifier for the thread executing this runnable. Used for tracking
        and debugging concurrent execution. Assigned at runtime by the executor.
    
    status : Literal["in_progress", "queued", "completed", "inactive"]
        Current execution status of the runnable. Possible states:
        - "inactive": Runnable has not been executed (default)
        - "queued": Runnable is waiting to be executed
        - "in_progress": Runnable is currently executing
        - "completed": Runnable has finished execution successfully
    
    scheduler : Optional[Scheduler]
        Optional scheduler for repeated or delayed execution. If set, the runnable
        executes repeatedly according to the scheduler's interval. Enables periodic
        jobs, delayed execution, or event-driven scheduling.
    
    Examples
    --------
    Using the factory pattern to create a runnable from an executable:
    
    >>> from transforms import ValueMapping
    >>> mapping_executable = ValueMapping(column='age', mapping={...}, default=0, alias='age_group')
    >>> MappingRunnable = Runnable.make(ValueMapping)
    >>> 
    >>> mapping_runnable = MappingRunnable(column='age', mapping={...}, default=0, alias='age_group')
    >>> result = mapping_runnable.run(df)
    
    With a scheduler for repeated execution:
    
    >>> from scheduler import Scheduler
    >>> extract_transform = ExtractTransform()
    >>> 
    >>> # Create runnable with scheduler
    >>> ExtractRunnable = Runnable.make(Extract)
    >>> extract_runnable = ExtractRunnable(source='api.json')
    >>> extract_runnable.scheduler = Scheduler(time_delta_in_seconds=3600)
    >>> 
    >>> # Will execute every hour and yield results
    >>> for result in extract_runnable.run(initial_data):
    ...     process(result)
    
    Combining runnables in a pipeline:
    
    >>> ExtractRunnable = Runnable.make(Extract)
    >>> TransformRunnable = Runnable.make(Transform)
    >>> LoadRunnable = Runnable.make(Load)
    >>> 
    >>> extract = ExtractRunnable(source='data.csv')
    >>> transform = TransformRunnable(transforms=[normalize, deduplicate])
    >>> load = LoadRunnable(destination='warehouse')
    >>> 
    >>> runner = Runner(runnables=[extract, transform, load])
    >>> result = runner.run(None)
    
    Notes
    -----
    - The factory pattern (`make()`) is the recommended way to create Runnables
    - Status is managed by the executor during pipeline execution
    - Thread IDs are assigned by concurrent executors for thread-safe tracking
    - Scheduled runnables yield multiple results over time; non-scheduled return single results
    - Any class with `run()` and `arun()` methods can be wrapped as a Runnable
    
    See Also
    --------
    Runner : Orchestrator that executes sequences of runnables
    Scheduler : Time-based scheduler for repeated execution
    Transform : Data transformation executable (example executable class)
    Executor : Manages execution of runnables with status tracking
    """
    def __init__(
        self,
        executable: Optional[Any] = None,
        thread_id: Optional[str] = None,
        status: Literal["in_progress", "queued", "completed", "inactive"] = "inactive",
        scheduler: Optional[Scheduler] = None,
    ):
            
        self.executable = executable 
        """
        Executable to be run by the Runner.
        
        The wrapped executable object that contains the actual execution logic.
        Should implement run() and arun() methods. Typically created via the
        make() factory method.
        """

        self.thread_id = thread_id
        """
        Thread id of the current runnable.
        
        Unique identifier assigned at runtime by concurrent executors for tracking
        which thread is executing this runnable. Used for debugging and monitoring
        concurrent execution.
        """

        self.status = status
        """
        Current status of the Runnable.
        
        Tracks the execution state:
        - "inactive": Has not been executed (default initial state)
        - "queued": Waiting in queue for execution
        - "in_progress": Currently executing
        - "completed": Finished execution successfully
        
        Managed by the executor during pipeline runs.
        """

        self.scheduler = scheduler
        """
        Schedule the next run after this many seconds, minutes, hours, etc.
        
        Optional scheduler for repeated or delayed execution. If set, run() will
        execute the runnable repeatedly according to the scheduler's interval,
        yielding results for each iteration. If None, runs once and returns result.
        """

    @classmethod
    def make(cls, bclass):
        # BUG <CRITICAL>: when creating a Runnable from a Executable
        # its name becomes "_Runnable", whereas the name should remain
        # the name of executable.
        # For eg - 

        # >> rename_columns_runner = Runner.make(RenameColumns(...))
        # >> print(rename_columns_runner.__name__)  
        #  _Runnable
        
        # whereas it should be "RenameColumns"
        # look into _Runnable.__init__()
        """
        Factory method to dynamically create a Runnable subclass from any executable class.
        
        This is the recommended pattern for wrapping existing executables. It combines
        the Runnable class with any class that implements a `run()` method, creating a
        new class that inherits from both. This allows seamless integration of existing
        executables into the Runner framework without modifying their source code.
        
        The resulting class can be instantiated like the original class but with all
        Runnable features (scheduling, status tracking, thread management).
        
        Parameters
        ----------
        bclass : type
            The base class to be wrapped. Must have a `run()` method. Typically an
            executable class like Transform, Downloader, Validator, Extract, Load, etc.
        
        Returns
        -------
        type
            A new class that inherits from both the provided class and Runnable.
            Can be instantiated with the same arguments as bclass.
        
        Raises
        ------
        AttributeError
            If the provided class does not have a `run()` method.
        
        Examples
        --------
        >>> from transforms import ValueMapping
        >>> MappingRunnable = Runnable.make(ValueMapping)
        >>> 
        >>> # Use exactly like the original class, but with Runnable features
        >>> runnable = MappingRunnable(column='status', mapping={'a': 1, 'b': 2})
        >>> result = runnable.run(df)
        
        >>> from downloaders import S3Downloader
        >>> S3Runnable = Runnable.make(S3Downloader)
        >>> 
        >>> s3_runnable = S3Runnable(bucket='my-bucket', prefix='data/')
        >>> s3_runnable.scheduler = Scheduler(time_delta_in_seconds=86400)  # Daily
        >>> for files in s3_runnable.run(None):
        ...     process_files(files)
        
        Notes
        -----
        - The factory creates a new class each time it's called
        - The resulting class initialization calls both parent constructors
        - Callable multiple times on the same class to create independent Runnable types
        """

        if not isinstance(bclass, type):
            # if it is an instance and not a class
            # we first get its class
            bclass = type(bclass)
        
        if not hasattr(bclass, "run"):
            raise AttributeError(f"Class {bclass.__name__} has no `run()` method")
        
        # checking the type of the class which needs to be made 
        # a runnable. Its meta or type should be `_ExecutableMeta`
        if not isinstance(bclass, _ExecutableMeta):
            raise TypeError(
                f"Root meta class of {bclass.__name__} is {get_root_metaclass(bclass)} ",
                # f"and root meta class of {cls.__name__} is {get_root_metaclass(cls)} ",
                "Root of this class should be subclass of `_ExecutableMeta` meta class ",
                "to be a made a runnable"
            )
        
        class _Runnable(bclass, cls):
            """
            Dynamically created subclass combining executable and Runnable functionality.
            """
            def __init__(self, *args, **kwargs):     
                bclass.__init__(self, *args, **kwargs)
                cls.__init__(self, executable=self)

        return _Runnable

    def run(self, x, *args, **kwargs):
        """
        Execute the runnable, optionally scheduling for repeated execution.
        
        Handles both scheduled and immediate execution:
        - If a scheduler is attached, delegates to the scheduler which handles
          repeated execution at specified intervals
        - If no scheduler, directly calls the executable's run() method once
        
        Parameters
        ----------
        x : Any
            Input data to pass to the executable. Can be None for operations
            that generate data (e.g., extraction from external sources).
        *args
            Additional positional arguments passed to the executable.
        **kwargs
            Additional keyword arguments passed to the executable.
        
        Returns
        -------
        Any (immediate execution) or Generator
            - If no scheduler: Returns the result from executable.run()
            - If scheduler attached: Yields results for each scheduled execution
        
        Yields
        ------
        Any
            When scheduler is present, yields the result from each scheduled execution.
        
        Examples
        --------
        Non-scheduled execution (single result):
        
        >>> runnable = SomeRunnable(config=...)
        >>> result = runnable.run(data)
        >>> print(result)  # Single output
        
        Scheduled execution (multiple results over time):
        
        >>> runnable = SomeRunnable(config=...)
        >>> runnable.scheduler = Scheduler(time_delta_in_seconds=300)  # Every 5 min
        >>> for result in runnable.run(data):
        ...     print(f"Got result at {datetime.now()}: {result}")
        
        Notes
        -----
        - Scheduled runnables block until each interval completes
        - For non-blocking scheduled execution, use with Runner that handles scheduling
        - Status updates happen in the Runner/Executor, not in this method
        """
        if self.scheduler:
            _scheduled_executable = self.scheduler.schedule(executable=self.executable)
            # perform the task after said time
            for res in _scheduled_executable(x, *args, **kwargs):
                yield res
        else:
            logger.debug("No scheduler for task=%s", str(self.executable))
            # no scheduling
            res = self.executable.run(x, *args, **kwargs)
            yield res

    async def arun(self, x, *args, **kwargs):
        """
        Asynchronous execution of the runnable.
        
        Provides async/await support for non-blocking execution within async contexts.
        Must be implemented by subclasses that support asynchronous operations.
        
        Parameters
        ----------
        x : Any
            Input data to pass to the executable.
        *args
            Additional positional arguments passed to the executable.
        **kwargs
            Additional keyword arguments passed to the executable.
        
        Raises
        ------
        NotImplementedError
            This is an abstract method that must be implemented by subclasses
            that support asynchronous execution.
        
        Notes
        -----
        - Called by async-capable executors and runners
        - Should follow the same logic as run() but use async/await
        - Consider using with Scheduler for non-blocking scheduled execution
        """
        return await self.executable.arun(x, *args, **kwargs)
    
    def __repr__(self):
        return f"Runnable@{self.executable}"
    

class RunnableNode:
    def __init__(self, runnable: Union[Runnable, "RunnableNode"], next: "RunnableNode"=None, prev: "RunnableNode"=None):
        if isinstance(runnable, RunnableNode):
            # Copy fields from existing node into this new self
            self._runnable = runnable._runnable
            self.next = runnable.next
            self.prev = runnable.prev
        else:
            self._runnable = runnable
            self.next = next
            self.prev = prev


