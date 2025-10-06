import logging

from .extract import *
from .transform import *
from .load import *
from .etl import Etl

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.DEBUG,
)


def main() -> None:
    print("Hello from etl!")
