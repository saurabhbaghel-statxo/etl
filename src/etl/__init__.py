import logging

# from .extract import *
# from .transform import *
# from .load import *
from ._etl import *
# from ._runnable import *
# from .policy import *
# from .executor import *
# from .scheduler import *
# from ._runner import *


# logging.basicConfig(
#     format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
#     level=logging.DEBUG,
# )


def main() -> None:
    print("Hello from etl!")
