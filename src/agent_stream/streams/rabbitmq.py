from typing import (AsyncGenerator, Callable, List, Optional, TypeVar,
                    Union, cast)

from autogen_agentchat.base import Response, TaskResult
from autogen_agentchat.messages import (BaseAgentEvent, BaseChatMessage,
                                        ModelClientStreamingChunkEvent,
                                        MultiModalMessage,
                                        UserInputRequestedEvent)
from autogen_core import CancellationToken
from autogen_core.models import RequestUsage

from ..filters import StreamFilter, resolve_filter
from ..managers import RabbitMQManager

T = TypeVar("T", bound=TaskResult | Response)

class RabbitMQStreamManager:
    def __init__(self, rabbitmq: RabbitMQManager, queue_name: str):
        self.rabbitmq: RabbitMQManager = rabbitmq
        self.queue_name = queue_name
        self.streaming_chunks: list[str] = []
        self.total_usage = RequestUsage(prompt_tokens=0, completion_tokens=0)

    async def send_message(self, content: str, source: str, message_type: str, end_of_stream: bool = False):
        await self.rabbitmq.send_stream_message(
            queue_name=self.queue_name,
            context_id=source,
            message_type=message_type,
            content=content,
            end_of_stream=end_of_stream,
            use_stream_queue=True
        )

    async def process_message(self, message: BaseAgentEvent | BaseChatMessage | T) -> Optional[T]:
        if isinstance(message, TaskResult):
            await self.send_message(
                content=f"Task completed: {message.stop_reason}",
                source="system",
                message_type="task_result",
                end_of_stream=True
            )
            return message
        elif isinstance(message, Response):
            content = message.chat_message.to_text()
            await self.send_message(
                content=content,
                source=message.chat_message.source,
                message_type="response",
                end_of_stream=True
            )
            if message.chat_message.models_usage:
                self.total_usage.completion_tokens += message.chat_message.models_usage.completion_tokens
                self.total_usage.prompt_tokens += message.chat_message.models_usage.prompt_tokens
            return message
        elif isinstance(message, UserInputRequestedEvent):
            await self.send_message(
                content="User input requested",
                source=message.source,
                message_type="input_request"
            )
            return None
        else:
            message = cast(BaseAgentEvent | BaseChatMessage, message)
            if isinstance(message, ModelClientStreamingChunkEvent):
                self.streaming_chunks.append(message.content)
                await self.send_message(
                    content=message.content,
                    source=message.source,
                    message_type="stream_chunk"
                )
            else:
                if self.streaming_chunks:
                    self.streaming_chunks.clear()
                if isinstance(message, MultiModalMessage):
                    content = message.to_text()
                else:
                    content = message.to_text()
                await self.send_message(
                    content=content,
                    source=message.source,
                    message_type="message"
                )
                if message.models_usage:
                    self.total_usage.completion_tokens += message.models_usage.completion_tokens
                    self.total_usage.prompt_tokens += message.models_usage.prompt_tokens
            return None

async def RabbitMQStream(
    stream: AsyncGenerator[BaseAgentEvent | BaseChatMessage | T, None],
    rabbitmq: RabbitMQManager,
    queue_name: str,
    *,
    cancellation_token: Optional[CancellationToken] = None,
    filters: Optional[Union[StreamFilter, List[StreamFilter], Callable[[object], bool], List[Callable[[object], bool]]]] = None,
) -> T:
    """
    Consumes the message stream and sends messages to RabbitMQ queue.
    Args:
        stream: Message stream to process
        rabbitmq: RabbitMQ manager instance (must provide send_stream_message)
        queue_name: Name of the queue to send messages to
        cancellation_token: Optional token to cancel the operation
        filters: Optional StreamFilter or list of StreamFilters (or callables). Each should take a message and return True if it should be streamed.
    Returns:
        The last processed TaskResult or Response
    """
    manager = RabbitMQStreamManager(rabbitmq, queue_name)
    last_processed: Optional[T] = None
    filter_fn = resolve_filter(filters)
    try:
        async for message in stream:
            if cancellation_token and cancellation_token.cancelled:
                break
            if not filter_fn(message) and not isinstance(message, TaskResult) and not isinstance(message, Response):
                continue
            result = await manager.process_message(message)
            if result is not None:
                last_processed = result
        return last_processed
    except Exception as e:
        raise RuntimeError(f"Error processing message stream: {str(e)}") 