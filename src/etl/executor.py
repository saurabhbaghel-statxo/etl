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

    async def _execute_single_runnable(self, runnable, x: List, *args, **kwargs):
        logger.debug("Executing: %s", runnable)
        if runnable.status in ["queued", "inactive"]:
            runnable.status = "in_progress"
        try:
            if asyncio.iscoroutinefunction(runnable.executable.run): 
                # NOTE: Have temporarily made this runnable.run -> runnable.executable.run
                # TODO: Check the difference
                y = await self._call_arun_executable(
                    runnable, x, *args, **kwargs
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
    


    # async def _execute_runnables_single_chain(self, runnables: List, x, *args, **kwargs):
    async def _execute_runnables_single_chain(self, root_runnable, x: List, *args, **kwargs):
        # root_runnable is RunnableNode
        runnable = root_runnable
        while runnable:
            # TODO: check runnable and allocate resource 
            # runnable = runnables.pop(0) # get the first one
            x = await self._execute_single_runnable(runnable._runnable, x, *args, **kwargs)
            runnable = runnable.next
        return x       

    def _make_runnables_queue(self, runnables: List): # RunnableNode
        from ._runnable import RunnableNode
        
        # first node

        root_node = RunnableNode(runnables[0])
        prev_node = root_node
        for _runnable_ in runnables[1:]:
            # assuming these are in a definite order
            curr_node = RunnableNode(_runnable_)            
            curr_node.prev = prev_node
            prev_node.next = curr_node
            prev_node = curr_node

        return root_node

    # async def execute(self, runnables: List, x: Optional[List], *args, **kwargs) -> Generator:
    async def execute(self, runnables: List, x: Optional[List], *args, **kwargs) -> Generator:
        from .policy import PolicyOptions
        from ._runnable import RunnableNode
        from copy import deepcopy
        
        # runnable is RunnableNode
        
        # making a copy of the runnables so
        # that when the popping happens for the current 
        # chain of executables it doesnt 
        # disturb the fresh chain of executables executing concurrently
        root_runnables_copy =  [RunnableNode(_runnable_) for _runnable_ in runnables]

        logger.info("Policy=%s", self._policy)
        if self._policy == PolicyOptions.default:
            # execute the runnables one by one
            # also, the take the input one by one

            # make a linked-list of the runnables
            root_runnable = self._make_runnables_queue(root_runnables_copy)

            return await self._execute_runnables_single_chain(root_runnable, x, *args, **kwargs)
        
        if self._policy == PolicyOptions.compute_optimized:
            logger.error("Compute Optimized policy not implemented yet.")
            # preferred for I/O ops
            raise NotImplementedError
        
        if self._policy == PolicyOptions.storage_optimized:
            logger.error("Storage Optimized policy not implemented yet.")
            raise NotImplementedError
        
        if self._policy == PolicyOptions.parallel:
            # run the root_nodes parallely
            tasks = []
            for __runnable__ in root_runnables_copy:
                task = asyncio.create_task(
                        self._execute_runnables_single_chain(
                            __runnable__, x, *args, **kwargs
                            )
                        )

            await asyncio.gather(*tasks)
        # # update the statuses of the runnables
        # self.runnables_statuses = {_runnable: _runnable.status for _runnable in runnables}
        