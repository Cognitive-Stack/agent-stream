# agent-stream

A modular Python package for real-time agent message streaming and filtering, designed for LLM/AI agent systems. Provides composable stream filters and streaming utilities for RabbitMQ and more.

## Features
- Composable stream filters (by function call, text, source, etc.)
- Pluggable streaming backends (RabbitMQ, more coming)
- Clean separation of filter logic and streaming logic
- Environment-variable-based configuration for managers
- Extensible for new message types and streaming backends

## Installation

```bash
pip install agent-stream
```

## Usage

### Importing in your code

You can use the package in your code as follows:

#### Example: Using RabbitMQManager in an application

```python
from agent_stream.managers import RabbitMQManager

rabbitmq_manager = RabbitMQManager()
await rabbitmq_manager.connect()
# ... use rabbitmq_manager ...
await rabbitmq_manager.close()
```

#### Example: Using filters and streams in an agent service with autogen

```python
from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.base import Handoff
from autogen_agentchat.teams import Swarm
from agent_stream.streams import RabbitMQStream
from agent_stream.filters import FunctionCallStreamFilter, StreamChunkStreamFilter
from agent_stream.managers import RabbitMQManager

# Define your agents and team (simplified example)
leader_agent = AssistantAgent(
    "leader_agent",
    tools=[],
    model_client=your_llm_client,
    model_client_stream=True,
    system_message="You are the leader.",
    handoffs=[Handoff(target="coder_agent", name="request_code_implementation")]
)
coder_agent = AssistantAgent(
    "coder_agent",
    tools=[],
    model_client=your_llm_client,
    model_client_stream=True,
    system_message="You are the coder.",
    handoffs=[Handoff(target="leader_agent", name="provide_implementation")]
)
team = Swarm([leader_agent, coder_agent])

# Initialize RabbitMQ manager
rabbitmq_manager = RabbitMQManager()
await rabbitmq_manager.connect()

# Run the team and stream results through RabbitMQStream with filters
result = await RabbitMQStream(
    stream=team.run_stream(task="Your task message"),
    rabbitmq=rabbitmq_manager,
    queue_name="agent-stream-demo",
    filters=[
        FunctionCallStreamFilter("signal_edit_file"),
        FunctionCallStreamFilter("signal_read_file"),
        StreamChunkStreamFilter("leader_agent"),
        StreamChunkStreamFilter("coder_agent")
    ]
)

await rabbitmq_manager.close()
```

### Environment Variables
- `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD` (for RabbitMQManager)

### Extending
- Add new filters in `filters/`
- Add new streaming backends in `streams/`
- Add new managers in `managers/`

### Filtering

Filtering in `agent-stream` allows you to process only the messages or events you care about from a stream of agent or LLM outputs. Filters are composable and can be combined using logical operators to create complex filtering logic.

#### Built-in Filters

- **FunctionCallStreamFilter**: Passes through only messages that represent a function/tool call with a specific name.
- **TextMessageStreamFilter**: Passes through only messages containing a specific text.
- **StreamChunkStreamFilter**: Passes through only messages from a specific agent or stream chunk.

#### Example: Creating and Combining Filters

```python
from agent_stream.filters import FunctionCallStreamFilter, TextMessageStreamFilter, StreamChunkStreamFilter

# Filter for tool call events with a specific function name
filter1 = FunctionCallStreamFilter("signal_edit_code")

# Filter for messages containing a specific text
filter2 = TextMessageStreamFilter("error")

# Filter for messages from a specific agent
filter3 = StreamChunkStreamFilter("leader_agent")

# Combine filters with | (OR) and & (AND)
combined_filter = (filter1 | filter2) & filter3
```

You can pass a single filter or a list of filters to `RabbitMQStream` or other streaming utilities. When multiple filters are provided, a message must pass **all** filters to be included (logical AND).

#### Example: Using Filters in a Stream

```python
from agent_stream.streams import RabbitMQStream
from agent_stream.filters import FunctionCallStreamFilter, StreamChunkStreamFilter

result = await RabbitMQStream(
    stream=your_async_stream,
    rabbitmq=rabbitmq_manager,
    queue_name="your-queue-name",
    filters=[
        FunctionCallStreamFilter("signal_edit_file"),
        StreamChunkStreamFilter("leader_agent")
    ]
)
```

This will only stream messages that are function calls to `signal_edit_file` **and** are from the `leader_agent`.

#### Custom Filters

You can create your own filters by subclassing the base filter class and implementing the `__call__` method.

```python
from agent_stream.filters import BaseStreamFilter

class CustomFilter(BaseStreamFilter):
    def __call__(self, message):
        # Your custom logic here
        return "important" in message.get("content", "")
```

---

## License
MIT

