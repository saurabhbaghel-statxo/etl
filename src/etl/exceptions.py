class CyclicityError(Exception):
    '''Is raised when there is cyclicity in the graph expected to be acyclic.'''
    pass

class BranchingWarning(Warning):
    '''Is raised when there is a branching detected in the graph at a node.'''
    pass

class BranchingError(Exception):
    """Is raised when there is a branching detected in the graph at a node.
    Currently, we raise an exception but in future this will be replaced with just a warning."""
    pass

class MissingTransformArgument(Exception):
    """Raised when the transform is initialized but the required arguments are missing."""
    pass

class FunctionalityNotFoundError(Exception):
    """Raised when the said transform is not implemented"""
    pass