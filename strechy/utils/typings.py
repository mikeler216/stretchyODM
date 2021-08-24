import typing
from typing import TypedDict


class Properties(TypedDict, total=False):
    properties: dict[str, typing.Any]


class Mappings(TypedDict, total=False):
    mappings: Properties


class _CreateAction(TypedDict, total=False):
    _id: str
    doc: dict[str, typing.Any]
    _index: str


class CreateAction(TypedDict, total=False):
    create: _CreateAction
