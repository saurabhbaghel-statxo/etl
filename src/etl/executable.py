# base class for every executable which 
# could be a standalone executable unit
# this could be made a runnable which then could be 
# run in according to a policy

# But this is just the basic executable unit

from abc import ABCMeta, abstractmethod
from typing import Optional, Any


class _ExecutableMeta(type, metaclass=ABCMeta):
    """
    Metaclass that enforces both synchronous and asynchronous execution methods.
    
    This serves as a mold for all executable components in the system (transforms,
    downloaders, validators, etc.), ensuring they support both execution models.
    
    Can only run in real time if executed alone.
    But if wrapped as a `Runnable` it could be scheduled
    """
    # _registry_ = {}

    def __new__(mcls, name, bases, namespace, **kwargs):
        # Register only concrete subclasses (not the base Extract)
        if ("run" not in namespace) and ("arun" not in namespace):
            namespace["run"] = abstractmethod(lambda self: None)
            namespace["arun"] = abstractmethod(lambda self: None)

            # raise TypeError(f"Type of {name} is {mcls.__name__} which mandates either `run()` or `arun()` to be implemented.")

        return super().__new__(mcls, name, bases, namespace)
    
    def __init__(cls, name, bases, namespace, **kwargs):
        # Consume kwargs here in __init__ instead
        super().__init__(name, bases, namespace)


    # @classmethod
    # def get_extractor(mcls, name):
    #     return mcls._registry_[name]
    
    @abstractmethod
    def run(self, *args, **kwargs):
        """Running logic of the basic executable"""
        ...

    @abstractmethod
    async def arun(self, *args, **kwargs):
        """Runs logic asynchronously"""
        ...

