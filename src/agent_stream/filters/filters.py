from typing import Any, Callable, List, Optional, Union

class StreamFilter:
    def __call__(self, message: Any) -> bool:
        raise NotImplementedError

    def __or__(self, other: 'StreamFilter') -> 'StreamFilter':
        return OrStreamFilter(self, other)

    def __and__(self, other: 'StreamFilter') -> 'StreamFilter':
        return AndStreamFilter(self, other)

class OrStreamFilter(StreamFilter):
    def __init__(self, *filters: 'StreamFilter'):
        self.filters = filters
    def __call__(self, message: Any) -> bool:
        return any(f(message) for f in self.filters)

class AndStreamFilter(StreamFilter):
    def __init__(self, *filters: 'StreamFilter'):
        self.filters = filters
    def __call__(self, message: Any) -> bool:
        return all(f(message) for f in self.filters)

def resolve_filter(filters: Optional[Union[StreamFilter, List[StreamFilter], Callable[[object], bool], List[Callable[[object], bool]]]]) -> Callable[[object], bool]:
    if filters is None:
        return lambda m: True
    if isinstance(filters, StreamFilter):
        return filters
    if callable(filters):
        return filters
    if isinstance(filters, list):
        if all(isinstance(f, StreamFilter) for f in filters):
            return OrStreamFilter(*filters)
        if all(callable(f) for f in filters):
            return lambda m: any(f(m) for f in filters)
    raise ValueError("Invalid filters argument for RabbitMQStream")