from typing import AsyncGenerator, Callable, List, Optional, TypeVar, Union

from autogen_agentchat.base import Response, TaskResult
from autogen_agentchat.messages import (
    BaseAgentEvent, BaseChatMessage, ModelClientStreamingChunkEvent,
    MultiModalMessage, UserInputRequestedEvent
)
from autogen_core import CancellationToken

from ..filters import StreamFilter, resolve_filter

T = TypeVar("T", bound=TaskResult | Response)

async def SSEStream(
    stream: AsyncGenerator[BaseAgentEvent | BaseChatMessage | T, None],
    *,
    cancellation_token: Optional[CancellationToken] = None,
    filters: Optional[Union[StreamFilter, List[StreamFilter], Callable[[object], bool], List[Callable[[object], bool]]]] = None,
) -> AsyncGenerator[str, None]:
    """
    Consumes the message stream and yields Server-Sent Events (SSE) formatted strings.
    Args:
        stream: Message stream to process
        cancellation_token: Optional token to cancel the operation
        filters: Optional StreamFilter or list of StreamFilters (or callables). Each should take a message and return True if it should be streamed.
    Yields:
        SSE-formatted strings (e.g., 'data: ...\n\n')
    """
    filter_fn = resolve_filter(filters)
    try:
        async for message in stream:
            if cancellation_token and cancellation_token.cancelled:
                break
            # Always yield TaskResult/Response, filter others
            if not filter_fn(message) and not isinstance(message, TaskResult) and not isinstance(message, Response):
                continue
            if isinstance(message, TaskResult):
                yield f"event: task_result\ndata: Task completed: {message.stop_reason}\nsource: system\n\n"
            elif isinstance(message, Response):
                content = message.chat_message.to_text()
                source = getattr(message.chat_message, 'source', 'unknown')
                yield f"event: response\ndata: {content}\nsource: {source}\n\n"
            elif isinstance(message, UserInputRequestedEvent):
                source = getattr(message, 'source', 'unknown')
                yield f"event: input_request\ndata: User input requested\nsource: {source}\n\n"
            elif isinstance(message, ModelClientStreamingChunkEvent):
                source = getattr(message, 'source', 'unknown')
                yield f"event: stream_chunk\ndata: {message.content}\nsource: {source}\n\n"
            elif isinstance(message, (BaseAgentEvent, BaseChatMessage)):
                # Handle MultiModalMessage as a special case
                if hasattr(message, 'to_text'):
                    content = message.to_text()
                else:
                    content = str(message)
                source = getattr(message, 'source', 'unknown')
                yield f"event: message\ndata: {content}\nsource: {source}\n\n"
                # Optionally yield usage info if present
                models_usage = getattr(message, 'models_usage', None)
                if models_usage:
                    yield f"event: usage\ndata: prompt_tokens={models_usage.prompt_tokens}, completion_tokens={models_usage.completion_tokens}\nsource: {source}\n\n"
            else:
                # Fallback for any other message types
                content = str(message)
                yield f"event: message\ndata: {content}\nsource: unknown\n\n"
    except Exception as e:
        yield f"event: error\ndata: Error processing message stream: {str(e)}\nsource: system\n\n" 
        yield f"event: error\ndata: Error processing message stream: {str(e)}\n\n" 