import asyncio
import json
import logging
import os
from typing import Any, Dict, Optional

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, DeliveryMode

logger = logging.getLogger(__name__)

class RabbitMQManager:
    """Generic RabbitMQ connection manager for agent streaming."""
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, username: Optional[str] = None, password: Optional[str] = None):
        self.host = host or os.getenv("RABBITMQ_HOST", "localhost")
        self.port = port or int(os.getenv("RABBITMQ_PORT", "5672"))
        self.username = username or os.getenv("RABBITMQ_USERNAME", "guest")
        self.password = password or os.getenv("RABBITMQ_PASSWORD", "guest")
        self.connection: Optional[AbstractConnection] = None
        self.channel: Optional[AbstractChannel] = None

    async def connect(self, max_retries: int = 5, retry_delay: int = 5):
        if self.connection is not None and not self.connection.is_closed:
            return
        retries = 0
        while retries < max_retries:
            try:
                self.connection = await aio_pika.connect_robust(
                    host=self.host,
                    port=self.port,
                    login=self.username,
                    password=self.password,
                )
                self.channel = await self.connection.channel()
                logger.info(f"Connected to RabbitMQ: amqp://{self.host}:{self.port}")
                return
            except Exception as e:
                retries += 1
                logger.warning(f"Failed to connect to RabbitMQ (attempt {retries}/{max_retries}): {str(e)}")
                if retries >= max_retries:
                    logger.error(f"Max retries reached, could not connect to RabbitMQ")
                    raise RuntimeError(f"Failed to connect to RabbitMQ after {max_retries} attempts: {str(e)}")
                await asyncio.sleep(retry_delay)

    async def close(self):
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            self.connection = None
            self.channel = None
            logger.info("Disconnected from RabbitMQ")

    async def declare_queue(self, queue_name: str, durable: bool = True, as_stream: bool = True):
        if self.channel is None:
            await self.connect()
        arguments = {}
        if as_stream:
            arguments["x-queue-type"] = "stream"
        queue = await self.channel.declare_queue(
            queue_name,
            durable=durable,
            arguments=arguments
        )
        return queue

    async def send_message(
        self,
        queue_name: str,
        message: Dict[str, Any],
        use_stream_queue: bool = True,
        end_of_stream: bool = False,
        persistent: bool = True
    ):
        if self.channel is None:
            await self.connect()
        await self.declare_queue(queue_name, as_stream=use_stream_queue)
        message_body = json.dumps(message).encode()
        headers = {"end-of-stream": end_of_stream}
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=message_body,
                delivery_mode=DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT,
                headers=headers
            ),
            routing_key=queue_name
        )
        logger.debug(f"Sent message to '{queue_name}'")
        return True

    async def send_stream_message(
        self,
        queue_name: str,
        context_id: str,
        message_type: str,
        content: str,
        action: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        state: Optional[str] = None,
        end_of_stream: bool = False,
        use_stream_queue: bool = True
    ):
        message = {
            "context_id": context_id,
            "type": message_type,
            "content": content
        }
        if action:
            message["action"] = action
        if params:
            message["params"] = params
        if state:
            message["state"] = state
        return await self.send_message(
            queue_name=queue_name,
            message=message,
            end_of_stream=end_of_stream,
            use_stream_queue=use_stream_queue
        ) 