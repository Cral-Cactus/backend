import functools
import re
from decimal import Decimal
from enum import Enum
from importlib import import_module
from types import FunctionType
from typing import Any, Optional, Type, Union

import click


def wrap_method(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        return method(self._impl, *args, **kwargs)

    return wrapped


class LazyClickMixin:

    _import_name: str
    _loaded_impl: Optional[Union[click.Command, click.Group]]

    def __init__(self, *, import_name, **kwargs):
        self._import_name = import_name
        self._loaded_impl = None
        super().__init__(**kwargs)
        for key, val in vars(type(self).__mro__[2]).items():
            if key.startswith("__"):
                continue
            if isinstance(val, FunctionType):
                setattr(self, key, wrap_method(val).__get__(self, self.__class__))

    @property
    def _impl(self):
        if self._loaded_impl:
            return self._loaded_impl
        module, name = self._import_name.split(":", 1)
        self._loaded_impl = getattr(import_module(module), name)
        return self._loaded_impl


class LazyGroup(LazyClickMixin, click.Group):
    pass


class EnumChoice(click.Choice):
    enum: Type[Enum]

    def __init__(self, enum: Type[Enum]):
        enum_members = [e.name for e in enum]
        super().__init__(enum_members)
        self.enum = enum

    def convert(self, value: Any, param, ctx):
        if isinstance(value, self.enum):
            return next(e for e in self.enum if e == value)
        value = super().convert(value, param, ctx)
        return next(k for k in self.enum.__members__.keys() if k == value)

    def get_metavar(self, param):
        name = self.enum.__name__
        name = re.sub(r"([A-Z\d]+)([A-Z][a-z])", r"\1_\2", name)
        name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
        return name.upper()


class MinMaxRangeParamType(click.ParamType):
    name = "min-max decimal range"

    def convert(self, value, param, ctx):
        try:
            left, _, right = value.partition(":")
            if left:
                left = Decimal(left)
            else:
                left = None
            if right:
                right = Decimal(right)
            else:
                right = None
            return left, right
        except (ArithmeticError, ValueError):
            self.fail(f"{value!r} contains an invalid number", param, ctx)

    def get_metavar(self, param):
        return "MIN:MAX"


MinMaxRange = MinMaxRangeParamType()