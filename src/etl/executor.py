# Executes the runnable according to the Policy

from typing import List, Optional, Dict, Generator
import inspect
import asyncio
import logging

logger = logging.getLogger(__name__)

class Executor:
    def __init__(self, policy):
        self._policy = policy
        self.runnables_statuses = {}

    def _call_run_executable(self, runnable, x, *args, **kwargs):
        # handles both generators and single time execution
        result = runnable.run(x, *args, **kwargs)
        if inspect.isgenerator(result):
            # it is a generator
            for res in result:
                y = res

        # elif inspect.isasyncgen(result):
        #     # if you ever support async runnables
        #     async for res in result:
        #         y = res
        
        else:
            # one-time execution
            y = result
        return y
    
    async def _call_arun_executable(self, runnable, x, *args, **kwargs):
        return await runnable.arun(x, *args, **kwargs)

    def _execute_single_runnable(self, runnable, x, *args, **kwargs):
        logger.debug("Executing: %s", runnable)
        if runnable.status in ["queued", "inactive"]:
            runnable.status = "in_progress"
        try:
            if asyncio.coroutines.iscoroutinefunction(runnable.run):
                y = asyncio.run(self._call_arun_executable(
                    runnable, x, *args, **kwargs
                    )
                )
            else:
                y = self._call_run_executable(runnable, x, *args, **kwargs)
            
            # update the status
            runnable.status = "completed"
        except Exception as exc:
            runnable.status = "inactive"
            logger.exception("Exception at %s: %s", str(runnable), exc)

        # update the runnable statuses
        self.runnables_statuses[runnable] = runnable.status
        return y
    


    def _execute_runnables_single_chain(self, runnables: List, x, *args, **kwargs):
        while runnables:
            # TODO: check runnable and allocate resource 
            runnable = runnables.pop(0) # get the first one
            x = self._execute_single_runnable(runnable, x, *args, **kwargs)
        return x       


    def execute(self, runnables: List, x: Optional[List], *args, **kwargs) -> Generator:
        from .policy import PolicyOptions
        
        logger.info("Policy=%s", self._policy)
        if self._policy == PolicyOptions.default:
            # execute the runnables one by one
            # also, the take the input one by one
            return self._execute_runnables_single_chain(runnables, x, *args, **kwargs)
        
        if self._policy == PolicyOptions.compute_optimized:
            logger.error("Compute Optimized policy not implemented yet.")
            # preferred for I/O ops
            raise NotImplementedError
        
        if self._policy == PolicyOptions.storage_optimized:
            logger.error("Storage Optimized policy not implemented yet.")
            raise NotImplementedError

        # # update the statuses of the runnables
        # self.runnables_statuses = {_runnable: _runnable.status for _runnable in runnables}
        