import typing
from typing import TypedDict


class Properties(TypedDict, total=False):
    properties: dict[str, typing.Any]


class Mappings(TypedDict, total=False):
    mappings: Properties
