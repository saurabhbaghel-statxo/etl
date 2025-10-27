# a common runner interface which is subclassed
# by all the runners be it for etl as a whole, for loading, transformations and extraction

# a runner should handle the actual execution and resource allocation part
# the resource allocation includes - 
# when the task being run requires low compute and is independent of some other task then,
# run them together, concurrently / parallely, [I think concurrency should be used here.]
# else, keep the usual sequential flow of the tasks as given by the user.

# for the flow orchestration we would be needing a dependency graph. 


from dataclasses import dataclass, field
from typing import  Protocol, List, Optional
import logging

from ._runnable import Runnable
from .policy import Policy, PolicyOptions 
from .executor import Executor 
from .scheduler import Scheduler
from .executable import _ExecutableMeta 

logger = logging.getLogger(__name__)

# @dataclass
class Runner(metaclass=_ExecutableMeta):
    """
    Base orchestrator for executing sequences of runnables with configurable policies.
    
    Runner manages the execution of ordered sequences of Runnable objects (or nested
    Runners) according to a specified execution policy (e.g., sequential, parallel).
    It abstracts away execution mechanics by delegating to an Executor, allowing
    subclasses to define custom execution logic while maintaining a consistent interface.
    
    This is a dataclass-based design that separates concerns: runnables are what to run,
    policies determine how to run them, and executors handle the actual execution.
    
    Attributes
    ----------
    runnables : List[Runnable | Runner]
        Ordered sequence of operations to execute. Can contain individual Runnable
        objects or nested Runner objects to support complex hierarchical workflows.
        Items are executed in order, with each runnable receiving the output of
        the previous one (depending on policy).
    
    policy : PolicyOptions, optional
        Execution policy governing how runnables are orchestrated. Options may include
        sequential (default), parallel, or custom policies. Defaults to PolicyOptions.default.
    
    executor : Optional[Executor], optional
        Executor instance responsible for executing runnables according to the policy.
        Lazily initialized on first run if not provided. Allows reuse of the same
        executor across multiple runs for performance optimization.
    
    Examples
    --------
    Basic sequential execution:
    
    >>> from runner import Runner, Runnable
    >>> step1 = SomeRunnable()
    >>> step2 = AnotherRunnable()
    >>> step3 = FinalRunnable()
    >>> 
    >>> runner = Runner(runnables=[step1, step2, step3])
    >>> result = runner.run(initial_data)
    
    Nested runners for hierarchical workflows:
    
    >>> # Create sub-runners for different stages
    >>> data_prep = Runner(runnables=[validate_data, clean_data])
    >>> processing = Runner(runnables=[transform_data, enrich_data])
    >>> final = Runner(runnables=[aggregate_data, export_data])
    >>> 
    >>> # Combine into master runner
    >>> master = Runner(runnables=[data_prep, processing, final])
    >>> result = master.run(raw_data)
    
    With custom execution policy:
    
    >>> runner = Runner(runnables=[task1, task2, task3])
    >>> runner.policy = PolicyOptions.parallel  # Execute tasks in parallel
    >>> result = runner.run(data)
    
    Notes
    -----
    - Runnables are executed in the order provided unless the policy specifies otherwise
    - The executor is initialized on first run with the current policy
    - Execution status of each runnable is tracked and accessible via executor.runnables_statuses
    - Runner is designed as a reusable orchestrator and can be subclassed for domain-specific
      execution logic (e.g., Etl for Extract-Transform-Load pipelines)
    
    See Also
    --------
    Runnable : Base interface for executable operations
    Executor : Handles actual execution of runnables according to policy
    PolicyOptions : Available execution policies for orchestrating runnables
    """
    

    def __init__(
            self,
            runnables: List[Runnable] | List["Runner"],
            policy: PolicyOptions=PolicyOptions.default,
            executor: Optional[Executor]=None
    ):
        
        self.runnables = runnables
        """
        List of all the runnables to execute.
        
        Can contain Runnable objects (individual operations) or nested Runner objects
        (sub-pipelines). Execution order follows the list order.
        """

        self.policy = policy
        """
        General policy to guide how the runnables are to be run.
        
        Determines execution model (e.g., sequential, parallel). Set after initialization
        to configure execution behavior. Defaults to PolicyOptions.default.
        """

        self.executor = executor
        """
        Executor to run the runnables with the policy.
        
        Lazily created on first run() call. Can be set manually to reuse an executor
        across multiple runs or to customize execution behavior.
        """

    def run(self, x, *args, **kwargs):
        """
        Execute all runnables in sequence according to the execution policy.
        
        Orchestrates execution of the runnables list by:
        1. Creating or reusing an Executor with the current policy
        2. Delegating actual execution to the executor
        3. Tracking execution status of each runnable
        4. Logging success/failure/incomplete status
        5. Returning the final result
        
        Parameters
        ----------
        x : Any
            Initial input data for the first runnable. Output of each runnable
            becomes input to the next (depending on policy).
        *args
            Additional positional arguments passed through to each runnable.
        **kwargs
            Additional keyword arguments passed through to each runnable.
        
        Returns
        -------
        Any
            Final output after all runnables have processed the data. The exact
            type depends on the runnables and the final operation performed.
        
        Notes
        -----
        - Executor is created on first run and reused for subsequent calls
        - Execution policy must be set before calling run() to take effect
        - Runnables status is logged at INFO level after execution completes
        - Status tracking distinguishes between completed, unsuccessful, and incomplete
          runnables for monitoring and debugging
        
        Examples
        --------
        >>> runner = Runner(runnables=[extract, transform, load])
        >>> result = runner.run(source_data)
        >>> # result contains final output from load operation
        
        >>> runner = Runner(runnables=[validate, process, export])
        >>> result = runner.run(raw_input, timeout=300, retry=3)
        >>> # Additional arguments passed to each runnable
        """

        # initialise policy
        # Using the default policy for now
        _policy = Policy(policy_option=self.policy)

        # initialize executor
        if not self.executor:
            self.executor = Executor(_policy)
        
        # execute the runnables
        y = self.executor.execute(self.runnables, x, *args, **kwargs)

        # successfuly completed runnables
        _successful_runnables = [
            _run 
            for _run, status 
            in self.executor.runnables_statuses.items() 
            if status=="completed"
        ]

        # unsuccessful runnables
        _unsuccesful_or_incomplete_runnables = [
            _run 
            for _run in self.executor.runnables_statuses 
            if _run not in _successful_runnables
        ]
        
        logger.info(
            "Successful Tasks=%s, " \
            "Failed/Incomplete Tasks=%s", 
            _successful_runnables, 
            _unsuccesful_or_incomplete_runnables
        )

        return y