import logging
from typing import Literal


def setup_logging(level=logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S%z',
    )
