# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2025 Vast Data Ltd.

import sys
import logging
from typing import Callable
from types import SimpleNamespace

logging.basicConfig(level=logging.INFO)

# Platform-agnostic colors implementation
NAMES = ["grey", "red", "green", "yellow", "blue", "magenta", "cyan", "white"]
COLORS = SimpleNamespace()


def get_pairs():
    for i, name in enumerate(NAMES):
        yield (name, str(30 + i))
        yield "intense_" + name, str(30 + i) + ";1"


def ansi(code):
    return f"\033[{code}m"


def ansi_color(code, s):
    return f"{ansi(code)}{s}{ansi(0)}"


def make_color_fn(code):
    return lambda s: ansi_color(code, s)


if sys.platform == "win32":
    import colorama

    colorama.init(strip=False)
for (name, code) in get_pairs():
    setattr(COLORS, name, make_color_fn(code))


class ColoredFormatter(logging.Formatter):
    """A logging.Formatter which prints colored WARNING and ERROR messages"""

    def get_level_message(self, record):
        if record.levelno <= logging.INFO:
            levelname = f"{COLORS.green(record.levelname)}:"
        elif record.levelno <= logging.WARNING:
            levelname = f"{COLORS.yellow(record.levelname)}:"
        else:
            levelname = f"{COLORS.red(record.levelname)}:"
        return f"{levelname: <12}"

    def format(self, record):
        if isinstance(record.msg, bytes):
            record.msg = record.msg.decode()
        message = super().format(record)
        return f"{self.get_level_message(record)} {message}"


def get_logger(name: str, color: Callable):
    logger = logging.getLogger(name=name)
    root_level = logging.getLogger().getEffectiveLevel()

    cho = logging.StreamHandler(sys.stdout)
    che = logging.StreamHandler(sys.stderr)

    logger.propagate = False
    if logger.hasHandlers():
        logger.handlers.clear()

    cho.addFilter(lambda record: record.levelno <= logging.INFO)
    delim = color("|")
    logger = logging.LoggerAdapter(logger, {"driver": f"{delim} {logger.name:^6} {delim}"})

    logger.setLevel(root_level)
    cho.setLevel(root_level)
    che.setLevel(logging.WARNING)
    formatter = ColoredFormatter("%(asctime)s %(driver)s %(message)s", "%Y-%m-%d %H:%M:%S")
    cho.setFormatter(formatter)
    che.setFormatter(formatter)

    logger.logger.addHandler(cho)
    logger.logger.addHandler(che)
    return logger
