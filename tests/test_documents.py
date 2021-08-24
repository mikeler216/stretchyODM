import datetime

import pydantic
import pytest

from tests.conftest import KeyWordModelStrechyDocument
from utils.index import _get_index_mappings
from utils.strechy_pydantic_types import KeyWord


@pytest.mark.asyncio
async def test_init_document(
    test_model,
    elasticsearch_client,
):
    await test_model.init_index(client=elasticsearch_client)

    index_meta = test_model._get_index_meta()

    assert index_meta.index_name == test_model.__name__.lower()

    assert await index_meta.client.ping()


@pytest.mark.asyncio
async def test_init_datefield(
    datetime_model,
    elasticsearch_client,
):
    await datetime_model.init_index(client=elasticsearch_client)


@pytest.mark.asyncio
async def test_insert_document(
    inited_model,
    elasticsearch_client_sync,
    get_id_cache,
):
    _init_document = await inited_model.insert_document(
        document=inited_model(
            var_1="str",
            var_2="str",
        )
    )
    assert _init_document.var_2 == "str"
    assert _init_document.var_1 == "str"

    index_meta = _init_document._get_index_meta()

    elasticsearch_client_sync.indices.refresh(index_meta.index_name)

    _init_document_from_db = await index_meta.client.get(
        index=index_meta.index_name,
        id=_init_document.id,
    )
    _source = _init_document_from_db["_source"]

    assert "id" not in _source

    assert _source["var_1"] == _init_document.var_1
    assert _source["var_2"] == _init_document.var_2

    get_id_cache["firstmodelstrechydocument"].append(_init_document.id)


def test_get_index_mappings(
    datetime_model,
):
    mappings = _get_index_mappings(model_class=datetime_model)

    assert mappings == {
        "mappings": {
            "properties": {
                "created": {"type": "date"},
                "var_1": {
                    "type": "keyword",
                },
                "var_2": {"type": "date"},
            }
        }
    }


def test_keyword_strechy_model(
    key_word_model,
):
    class _TestKeyWordString(pydantic.BaseModel):
        var_1: KeyWord

    _test = _TestKeyWordString(
        var_1="1",
    )

    assert _test.dict() == {"var_1": "1"}

    assert key_word_model.__fields__["var_1"].type_ is KeyWord

    assert issubclass(key_word_model.__fields__["var_1"].type_, str)


_model_for_get = KeyWordModelStrechyDocument(
    var_1="string",
    var_2=datetime.datetime(
        year=2000, month=1, day=1, hour=1, minute=1, second=1
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    argnames=[
        "insert_model",
    ],
    indirect=[
        "insert_model",
    ],
    argvalues=[
        (_model_for_get,),
    ],
)
async def test_get_model(insert_model):
    _get_model = await KeyWordModelStrechyDocument.get(
        document_id=_model_for_get.id
    )

    assert _get_model.id == _model_for_get.id
    assert _get_model.var_1 == _model_for_get.var_1
    assert _get_model.var_2 == _model_for_get.var_2


@pytest.mark.asyncio
async def test_bulk_insert(
    inited_model,
):
    model = inited_model(
        var_1="1",
        var_2="2",
    )
    response = await inited_model.bulk_insert_document(
        documents=[model for _ in range(10)]
    )
    assert len(response.success) == 10
    assert response.errors == []
