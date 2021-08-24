import dataclasses
import datetime
import functools
import typing
from typing import Type, TypeVar

import elasticsearch7
import pydantic

from utils.exceptions import IndexWasNotInitialized
from utils.index import IndexMeta, set_index_meta
from utils.typings import CreateAction

DocType = TypeVar("DocType", bound="StrechyDocument")


@dataclasses.dataclass(init=True)
class BulkInsertResults:
    errors: list[tuple[Type["StrechyDocument"], dict[str, typing.Any]]]
    success: list[Type["StrechyDocument"]]


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
        _document = _db_return_value["_source"]
        _document["id"] = _db_return_value.pop("_id")
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
            raise TypeError(f"document must be of type: {cls.__name__}")

        _index_meta = cls._get_index_meta()
        _date_created = datetime.datetime.utcnow()
        document.created = _date_created
        _db_return = await _index_meta.client.index(
            index=_index_meta.index_name,
            body=document.json(
                exclude={"id"} if document.id is None else None
            ),
            id=document_id,
        )
        document.id = _db_return["_id"]
        return document

    @staticmethod
    def _bulk_create_actions(
        documents: list[DocType], index_name: str
    ) -> list[typing.Union[CreateAction, dict[str, typing.Any]]]:
        """

        :param documents:
        :return:
        """
        _create_actions: list[
            typing.Union[CreateAction, dict[str, typing.Any]]
        ] = []
        for document in documents:
            _create_action: CreateAction = {
                "create": {
                    "_index": index_name,
                }
            }
            if document.id is not None:
                _create_action["create"].update({"_id": document.id})
            _create_actions.append(_create_action)
            _create_actions.append(document.dict(exclude={"id"}))
        return _create_actions

    @classmethod
    async def bulk_insert_document(
        cls,
        documents: list[DocType],
    ) -> BulkInsertResults:
        _index_meta = cls._get_index_meta()

        for _document in documents:
            _document.created = datetime.datetime.utcnow()
        _create_actions = cls._bulk_create_actions(
            documents=documents, index_name=_index_meta.index_name
        )

        _db_return = await _index_meta.client.bulk(
            body=_create_actions,
        )

        _bulk_insert_results = BulkInsertResults(success=[], errors=[])

        for _document, _item in zip(documents, _db_return["items"]):
            if _item["create"]["status"] == 201:
                _document.id = _item["create"]["_id"]
                _bulk_insert_results.success.append(_document)
            else:
                _bulk_insert_results.errors.append(
                    (_document, _item["create"]["error"])
                )

        return _bulk_insert_results

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
        setattr(cls, "IndexMeta", _index_meta)

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
