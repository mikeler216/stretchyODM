import datetime
from typing import Type

import elasticsearch7
import pydantic

from utils.strechy_pydantic_types import KeyWordString


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
                'mappings': mappings['mappings'],
            }

        )
    except elasticsearch7.exceptions.RequestError as ex:
        if (
                ex.info['error']['root_cause'][0]['type']
                != 'resource_already_exists_exception'
        ):
            raise ex
    return _index_meta


def _get_index_mappings(
    model_class: Type[pydantic.BaseModel],
):
    mappings = {
        'mappings': {
            'properties': {}
        }

    }
    for field in model_class.__fields__.values():
        if field.type_ is datetime.datetime:
            mappings['mappings']['properties'].update({
                field.name: {'type': 'date'}}
            )
        elif field.type_ is KeyWordString:
            mappings['mappings']['properties'].update({
                field.name: {'type': 'keyword'}}
            )
        # create datetime automatic mapping
        if field.name in {
            'date_created'
        }:
            mappings['mappings']['properties'].update({
                field.name: {'type': 'date'}}
            )



    return mappings


async def init_strechy(
        client: elasticsearch7.AsyncElasticsearch,
        documents: list,
):
    for document in documents:
        await document.init_index(
            client=client,
        )
