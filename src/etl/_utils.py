import sys
import logging


class StatxoLogger(logging.Logger):
    def __init__(self, name, level = 0):
        super().__init__(name, level)
        self.handlers.append(sys.stdin)



logger = StatxoLogger(name="statxo-logger")