import datetime
from typing import Type

import elasticsearch7
import pydantic

from utils.strechy_pydantic_types import (
    Byte,
    Integer,
    KeyWord,
    Long,
    Short,
)
import utils.typings


class IndexMeta:
    def __init__(
        self,
        client: elasticsearch7.AsyncElasticsearch,
        index_name: str = None,
    ):
        self.index_name = index_name
        self.client = client


async def set_index_meta(
    client: elasticsearch7.AsyncElasticsearch,
    index_name: str,
    model_class: Type[pydantic.BaseModel],
) -> IndexMeta:
    _index_meta = IndexMeta(
        client=client,
        index_name=index_name,
    )
    try:
        mappings = _get_index_mappings(model_class=model_class)

        await _index_meta.client.indices.create(
            index=_index_meta.index_name,
            body={
                "mappings": mappings["mappings"],
            },
        )
    except elasticsearch7.exceptions.RequestError as ex:
        if (
            ex.info["error"]["root_cause"][0]["type"]
            != "resource_already_exists_exception"
        ):
            raise ex
    return _index_meta


def update_mappings(
    mappings: utils.typings.Mappings, field_name: str, field_type: str
) -> None:
    """

    :param mappings:
    :param field_name:
    :param field_type:
    :return:
    """
    mappings["mappings"]["properties"].update(
        {field_name: {"type": field_type}}
    )
    return None


def _get_index_mappings(
    model_class: Type[pydantic.BaseModel],
):
    """
    Get elasticsearch field mapped by using the pydantic type
    :param model_class: StrechyDocument
    :return: The mapping of the field types if a
    field type is unknown it will leave elasticsearch to decide
    """
    mappings: utils.typings.Mappings = {"mappings": {"properties": {}}}
    for field in model_class.__fields__.values():
        if field.type_ is datetime.datetime:
            update_mappings(
                mappings=mappings, field_name=field.name, field_type="date"
            )
        elif field.type_ is KeyWord:
            update_mappings(
                mappings=mappings, field_name=field.name, field_type="keyword"
            )
        elif field.type_ is bool:
            update_mappings(
                mappings=mappings, field_name=field.name, field_type="boolean"
            )
        elif field.type_ is Long:
            update_mappings(
                mappings=mappings, field_name=field.name, field_type="long"
            )
        elif field.type_ is Integer:
            update_mappings(
                mappings=mappings, field_name=field.name, field_type="integer"
            )
        elif field.type_ is Short:
            update_mappings(
                mappings=mappings, field_name=field.name, field_type="short"
            )
        elif field.type_ is Byte:
            update_mappings(
                mappings=mappings, field_name=field.name, field_type="byte"
            )

            # create datetime automatic mapping
        if field.name in {"date_created"}:
            mappings["mappings"]["properties"].update(
                {field.name: {"type": "date"}}
            )

    return mappings


async def init_strechy(
    client: elasticsearch7.AsyncElasticsearch,
    documents: list,
):
    """

    :param client: The elasticsearch Async client
    :param documents: A list of documents Types to init them with the client
    :return: None
    """
    for document in documents:
        await document.init_index(
            client=client,
        )
    return None
