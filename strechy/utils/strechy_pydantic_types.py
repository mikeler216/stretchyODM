from typing import TYPE_CHECKING

from pydantic.validators import str_validator

if TYPE_CHECKING:
    KeyWordString = str
else:

    class KeyWordString(
        str,
    ):
        ...

        @classmethod
        def __get_validators__(cls):
            yield str_validator
            yield cls.validate

        @classmethod
        def validate(cls, v: str):
            if not isinstance(v, str):
                raise TypeError("str type required")
            return str(v)
