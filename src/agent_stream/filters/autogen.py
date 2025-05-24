from typing import Any

from autogen_agentchat.messages import (BaseAgentEvent, BaseChatMessage,
                                        ModelClientStreamingChunkEvent,
                                        ToolCallExecutionEvent)

from .filters import StreamFilter


class FunctionCallStreamFilter(StreamFilter):
    def __init__(self, function_name: str):
        self.function_name = function_name
    def __call__(self, message: Any) -> bool:
        if isinstance(message, ToolCallExecutionEvent):
            return any(r.name == self.function_name for r in message.content)
        return False
    
class TextMessageStreamFilter(StreamFilter):
    def __init__(self, text: str):
        self.text = text
    def __call__(self, message: Any) -> bool:
        if isinstance(message, BaseChatMessage):
            return self.text in message.to_text()
        return False

class SourceMatchStreamFilter(StreamFilter):
    def __init__(self, source: str):
        self.source = source
    def __call__(self, message: Any) -> bool:
        if isinstance(message, BaseAgentEvent) or isinstance(message, BaseChatMessage):
            return message.source == self.source
        return False
    
class StreamChunkStreamFilter(StreamFilter):
    def __init__(self, source: str):
        self.source = source
    def __call__(self, message: Any) -> bool:
        if isinstance(message, ModelClientStreamingChunkEvent):
            return message.source == self.source
        return False 