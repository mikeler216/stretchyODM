from typing import TYPE_CHECKING

from pydantic.validators import int_validator, str_validator

if TYPE_CHECKING:
    KeyWord = str
    Long = int
    Short = int
    Byte = int
    Integer = int
else:

    class KeyWord(
        str,
    ):
        """
        Type representing keyword(str) in Elasticsearch
        """

        ...

        @classmethod
        def __get_validators__(cls):
            yield str_validator
            yield cls.validate

        @classmethod
        def validate(cls, v: str) -> str:
            if not isinstance(v, str):
                raise TypeError("str type required")
            return str(v)

    class BaseIntegerField(
        int,
    ):
        """
        Base for integer fields in Elasticsearch
        """

        gte = 1
        lte = 1

        @classmethod
        def __get_validators__(cls):
            yield int_validator
            yield cls.validate

        @classmethod
        def validate(cls, v: int) -> int:
            if v <= cls.gte or v >= cls.lte:
                raise ValueError(f"Value out of range {cls.gte} - {cls.lte}")
            if not isinstance(v, int):
                raise TypeError("int type required")
            return int(v)

    class Long(
        BaseIntegerField,
    ):
        """
        Type representing a long type in Elasticsearch
        """

        gte = -(2 ** 63)
        lte = 2 ** 63 - 1

    class Integer(
        BaseIntegerField,
    ):
        """
        Type representing an integer type in Elasticsearch
        """

        gte = -(2 ** 31)
        lte = 2 ** 31 - 1

    class Short(
        BaseIntegerField,
    ):
        """
        Type representing short type type in Elasticsearch
        """

        gte = -32768
        lte = 32768

    class Byte(
        BaseIntegerField,
    ):
        """
        Type representing byte type in Elasticsearch
        """

        gte = -128
        lte = 128
