import asyncio
import datetime
from typing import Type, TypedDict

import elasticsearch7
import pydantic
import pytest

from documents import StrechyDocument
from utils.strechy_pydantic_types import KeyWord


class RunIdCacheType(TypedDict):
    firstmodelstrechydocument: list[str]


_RUN_ID_CACHE: RunIdCacheType = {"firstmodelstrechydocument": []}


@pytest.fixture(scope="session")
def get_id_cache() -> RunIdCacheType:
    return _RUN_ID_CACHE


class Config(
    pydantic.BaseSettings,
):
    ELASTICSEARCH_HOSTS: str
    ELASTICSEARCH_PORT: str


@pytest.fixture(
    scope="session",
)
def app_config():
    yield Config()


@pytest.mark.asyncio
@pytest.fixture(
    scope="session",
)
async def elasticsearch_client(
    app_config,
):
    _elasticsearch_client = elasticsearch7.AsyncElasticsearch(
        hosts=[
            f"{app_config.ELASTICSEARCH_HOSTS}:{app_config.ELASTICSEARCH_PORT}"
        ],
    )
    yield _elasticsearch_client
    await _elasticsearch_client.close()


@pytest.mark.asyncio
@pytest.fixture(
    scope="session",
)
def elasticsearch_client_sync(
    app_config,
):
    _elasticsearch_client = elasticsearch7.Elasticsearch(
        hosts=[
            f"{app_config.ELASTICSEARCH_HOSTS}:{app_config.ELASTICSEARCH_PORT}"
        ],
    )
    yield _elasticsearch_client
    _elasticsearch_client.close()


@pytest.mark.asyncio
@pytest.fixture(scope="function", autouse=True)
async def delete_documents(
    get_id_cache,
    elasticsearch_client,
):
    yield
    if get_id_cache["firstmodelstrechydocument"]:
        _db_return = await elasticsearch_client.delete_by_query(
            index="firstmodelstrechydocument",
            body={
                "query": {
                    "ids": {
                        "values": get_id_cache["firstmodelstrechydocument"],
                    },
                },
            },
        )
        assert _db_return["deleted"] > 0

    get_id_cache["firstmodelstrechydocument"] = []


@pytest.fixture(
    autouse=True,
    scope="session",
)
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


class FirstModelStrechyDocument(
    StrechyDocument,
):
    var_1: str
    var_2: str


@pytest.mark.asyncio
@pytest.fixture
def test_model() -> Type[FirstModelStrechyDocument]:
    return FirstModelStrechyDocument


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def inited_model(
    test_model,
    elasticsearch_client,
) -> Type[FirstModelStrechyDocument]:
    await test_model.init_index(
        client=elasticsearch_client,
    )
    yield test_model

    setattr(test_model, "IndexMeta", None)


class DateTimeModelStrechyDocument(
    StrechyDocument,
):
    var_1: KeyWord
    var_2: datetime.datetime


class KeyWordModelStrechyDocument(
    StrechyDocument,
):
    var_1: KeyWord
    var_2: datetime.datetime

    def __len__(self):
        return 1


@pytest.fixture(scope="function")
def datetime_model() -> Type[DateTimeModelStrechyDocument]:
    return DateTimeModelStrechyDocument


@pytest.fixture(scope="function")
def key_word_model() -> Type[KeyWordModelStrechyDocument]:
    return KeyWordModelStrechyDocument


@pytest.mark.asyncio
@pytest.fixture
async def insert_model(request, elasticsearch_client):
    model: StrechyDocument = request.param
    await model.init_index(client=elasticsearch_client)
    await model.insert_document(document=model)
    yield
    index_meta = model._get_index_meta()
    await index_meta.client.indices.delete(index=index_meta.index_name)
