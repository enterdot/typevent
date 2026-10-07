# typevent

A minimal, typed event primitive.

## Install

```sh
pip install typevent
```

Requires Python 3.11+.

## Usage

typevent ships two event types: `Event` for sync code and `AsyncEvent` for
async code. Both fan one trigger call out to every subscribed callable of the
form `f(caller, payload)`.

### Event

```python
from typing import TypedDict

from typevent import Event


class BellPayload(TypedDict):
    times: int
    level: str


bell_rang: Event[str, BellPayload] = Event()


def acknowledge_bell(caller: str, payload: BellPayload) -> None:
    print(f"The {caller} rang the bell {payload['times']} times, it was {payload['level']}.")


bell_rang.subscribe(acknowledge_bell)

bell_rang("priest", {"times": 3, "level": "loud"})  # event(...) aliases trigger
bell_rang.trigger("nun", {"times": 5, "level": "pleasant"})
```

```console
The priest rang the bell 3 times, it was loud.
The nun rang the bell 5 times, it was pleasant.
```

### AsyncEvent

Subscribers may be sync or async; `trigger` awaits them in subscription order,
so a subscriber never observes side effects from a later one.
Triggering requires a running event loop.

```python
import asyncio
from typing import TypedDict

from typevent import AsyncEvent


class BellPayload(TypedDict):
    times: int
    level: str


bell_rang_async: AsyncEvent[str, BellPayload] = AsyncEvent()


def acknowledge_bell(caller: str, payload: BellPayload) -> None:
    print(f"The {caller} rang the bell {payload['times']} times, it was {payload['level']}.")


async def acknowledge_bell_async(caller: str, payload: BellPayload) -> None:
    print(f"(async) The {caller} rang the bell {payload['times']} times, it was {payload['level']}.")


bell_rang_async.subscribe(acknowledge_bell)
bell_rang_async.subscribe(acknowledge_bell_async)


async def main() -> None:
    await bell_rang_async("nun", {"times": 5, "level": "pleasant"})


asyncio.run(main())
```

```console
The nun rang the bell 5 times, it was pleasant.
(async) The nun rang the bell 5 times, it was pleasant.
```

## CancelEvent

Raising `CancelEvent` in a subscriber stops the fan-out: the remaining
subscribers are not called and no error reaches the caller.

## Overlapping async work

Subscribers run one at a time. To overlap work, subscribe a single async
callable that runs a group of handlers in parallel:

```python
async def log_and_archive(caller, payload):
    await asyncio.gather(
        log_bell(caller, payload),
        archive_bell(caller, payload),
    )

bell_rang_async.subscribe(log_and_archive)
```

The group is one subscriber as far as the event is concerned: it occupies
its subscription slot and a failure inside it aborts the fan-out like any
subscriber failure. The group's own failure policy (*e.g.* `gather`'s default,
`return_exceptions=True`, `asyncio.TaskGroup`) is the user's choice.

A sync handler doing blocking I/O runs inline on the loop; offload it inside
a group with `asyncio.to_thread`.
