# guides how does the current runnable should run the tasks
# default it should sequential
# it could be compute optimized too


from typing import Literal, List, Union
from enum import Enum

class PolicyOptions(Enum):
    compute_optimized=1
    '''Arranging the tasks to optimize the compute'''

    storage_optimized=2
    '''Arranging the tasks to optimize the storage from the runnables'''
    
    default=3
    '''Default policy, which assumes the sequential policy provided by the user'''


class Policy:
    """Policy to be followed while execution"""
    def __init__(self, policy_option: PolicyOptions=PolicyOptions.default):
        """
        _summary_

        Parameters
        ----------
        policy_option : PolicyOptions, optional
            _description_, by default PolicyOptions.default
        """
        self._policy_option = policy_option

    def rearrange_runnables(self, runnables: List, *args, **kwargs) -> List:
        """Rearranges the runnables according to the policy"""

        if self._policy_option == PolicyOptions.default:
            
            # take the list of the runnables as is
            return runnables
        
    def __repr__(self) -> str:
        return f"{self._policy_option}"
    
    def __eq__(self, other) -> bool:
        if not isinstance(other, (Policy, PolicyOptions)):
            return NotImplemented
        
        if type(other) is PolicyOptions:
            return self._policy_option == other
        else:
            # it is a Policy itself
            return self._policy_option == other._policy_option