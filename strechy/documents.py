import datetime
import functools
import typing
from typing import TypeVar, Type

import elasticsearch7
import pydantic

from utils.exceptions import IndexWasNotInitialized
from utils.index import set_index_meta, IndexMeta

DocType = TypeVar("DocType", bound="StrechyDocument")


class StrechyDocument(
    pydantic.BaseModel,
):
    id: typing.Optional[str] = None
    created: typing.Optional[datetime.datetime] = None

    def __init__(
        self,
        **kwargs,
    ):
        super(StrechyDocument, self).__init__(**kwargs)

    @functools.cached_property
    def _index_meta(
            self,
    ) -> IndexMeta:
        return StrechyDocument._get_index_meta()

    @classmethod
    async def get(
        cls: Type[DocType],
        document_id: str,
    ) -> DocType:
        _index_meta: IndexMeta = cls._get_index_meta()
        _db_return_value: dict[str, typing.Any] = await _index_meta.client.get(
            index=_index_meta.index_name,
            id=document_id,
        )
        _document = _db_return_value['_source']
        _document['id'] = _db_return_value.pop('_id')
        return cls(
            **_document,
        )

    @classmethod
    async def insert_document(
        cls,
        document: DocType,
        document_id: typing.Optional[str] = None,
    ) -> DocType:
        if type(document) is not cls:
            raise TypeError(f'document must be of type: {cls.__name__}')

        _index_meta = cls._get_index_meta()
        _date_created = datetime.datetime.utcnow()
        document.created = _date_created
        _db_return = await _index_meta.client.index(
            index=_index_meta.index_name,
            body=document.json(exclude={'id'}),
            id=document_id,
        )
        document.id = _db_return['_id']
        return document

    @classmethod
    async def init_index(
            cls: Type[DocType],
            client: elasticsearch7.AsyncElasticsearch,
    ):
        _index_meta = await set_index_meta(
            client=client,
            index_name=cls.__name__.lower(),
            model_class=cls,
        )
        setattr(cls, 'IndexMeta', _index_meta)

    @classmethod
    def _get_index_meta(
        cls: Type[DocType],
    ) -> IndexMeta:
        """

        :return:
        """
        collection_meta = getattr(cls, "IndexMeta", None)
        if collection_meta is None:
            raise IndexWasNotInitialized()
        return collection_meta
