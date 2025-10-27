# generic task which should be run by the runner,
# most of the time this would be a wrapper around the pipe
# but it should be made clear that the runner runs a task

from abc import ABCMeta, abstractmethod
from typing import Any, Optional
from dataclasses import dataclass, field

from ._utils import generate_random_id


class _TaskBackendMeta(ABCMeta):
    @abstractmethod
    def run(self, *args, **kwargs):
        """Run logic of the Task"""
        ...

@dataclass
class Task:
    task: Any
    '''Actual task, should be a pipe for now'''   
    
    name: str = field(
        default_factory=generate_random_id, 
        init=False, 
        metadata={"desc": "Unique name of the task"}
    )
    '''Unique name of the task'''

    allocator: Any = field(default=None, init=False)
    '''Allocates the runtime attention as per the memory required. Yet to be implemented'''




