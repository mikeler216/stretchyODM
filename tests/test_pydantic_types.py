import pydantic
import pytest

from utils import strechy_pydantic_types


def test_strechy_pydantic_types():
    class IntegerTypes(pydantic.BaseModel):
        x1: strechy_pydantic_types.Long
        x2: strechy_pydantic_types.Short
        x3: strechy_pydantic_types.Byte
        x4: strechy_pydantic_types.Integer

    with pytest.raises(pydantic.ValidationError):
        IntegerTypes(
            x1=9223372036854775807 + 1,
            x2=1,
            x3=1,
            x4=1,
        )
    with pytest.raises(pydantic.ValidationError):
        IntegerTypes(
            x1=1,
            x2=1,
            x3=1,
            x4=2147483647 + 1,
        )
    with pytest.raises(pydantic.ValidationError):
        IntegerTypes(
            x1=1,
            x2=65535 + 1,
            x3=1,
            x4=1,
        )
    with pytest.raises(pydantic.ValidationError):
        IntegerTypes(
            x1=1,
            x2=1,
            x3=256,
            x4=1,
        )

    assert IntegerTypes(x1=1, x2=1, x3=1, x4=1,).dict() == {
        "x1": 1,
        "x2": 1,
        "x3": 1,
        "x4": 1,
    }
