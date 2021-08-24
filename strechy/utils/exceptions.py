class StrechyExceptionsBase(Exception):
    pass


class IndexWasNotInitializedError(StrechyExceptionsBase):
    """
    Error StrechyDocument Type was accessed before Index init
    """

    pass
