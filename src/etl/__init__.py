import logging

from .transform import *

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.DEBUG,
)


def main() -> None:
    print("Hello from etl!")
