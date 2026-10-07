"""Tests for the typevent package."""

import asyncio
from typing import TypedDict

import pytest

from typevent import AsyncEvent, CancelEvent, Event


class PingPayload(TypedDict):
    """Payload used throughout the tests."""

    n: int


EventClass = type[Event[str, PingPayload]] | type[AsyncEvent[str, PingPayload]]


def fire(
    event: Event[str, PingPayload] | AsyncEvent[str, PingPayload],
    caller: str,
    payload: PingPayload,
) -> None:
    """Trigger ``event``; runs a temporary loop for :class:`AsyncEvent`."""
    outcome = event(caller, payload)
    if outcome is not None:
        asyncio.run(outcome)


@pytest.fixture(params=[Event, AsyncEvent], ids=["Event", "AsyncEvent"])
def event_cls(request: pytest.FixtureRequest) -> EventClass:
    """Yield both event classes, parametrized."""
    return request.param


def test_trigger_calls_all_subscribers_in_order(event_cls: EventClass) -> None:
    """Subscribers are invoked in subscription order with caller and payload."""
    event = event_cls()
    order: list[str] = []
    seen: list[tuple[str, PingPayload]] = []

    def first(caller: str, payload: PingPayload) -> None:
        order.append("first")
        seen.append((caller, payload))

    def second(caller: str, payload: PingPayload) -> None:
        order.append("second")

    event += first
    event += second

    fire(event, "tester", {"n": 1})

    assert order == ["first", "second"]
    assert seen == [("tester", {"n": 1})]


def test_subscribe_is_idempotent(event_cls: EventClass) -> None:
    """Subscribing the same callback twice registers it once."""
    event = event_cls()

    def handler(caller: str, payload: PingPayload) -> None:
        pass

    event += handler
    event += handler

    assert len(event) == 1


def test_unsubscribe_of_unknown_callback_is_noop(event_cls: EventClass) -> None:
    """Unsubscribing a callback that was never registered does nothing."""
    event = event_cls()

    def handler(caller: str, payload: PingPayload) -> None:
        pass

    event -= handler

    assert len(event) == 0


def test_unsubscribe_removes_callback(event_cls: EventClass) -> None:
    """Unsubscribed callbacks are no longer called."""
    event = event_cls()
    order: list[str] = []

    def first(caller: str, payload: PingPayload) -> None:
        order.append("first")

    def second(caller: str, payload: PingPayload) -> None:
        order.append("second")

    event += first
    event += second
    event -= first

    fire(event, "tester", {"n": 1})

    assert order == ["second"]


def test_len_and_repr(event_cls: EventClass) -> None:
    """len() counts subscribers; repr() shows the class and the count."""
    event = event_cls()

    def handler(caller: str, payload: PingPayload) -> None:
        pass

    assert repr(event) == f"{event_cls.__name__}(subscribers=0)"
    event += handler
    event += handler
    assert len(event) == 1
    assert repr(event) == f"{event_cls.__name__}(subscribers=1)"


def test_clear_removes_all_subscribers(event_cls: EventClass) -> None:
    """clear() drops every subscriber."""
    event = event_cls()
    calls: list[str] = []

    def handler(caller: str, payload: PingPayload) -> None:
        calls.append("handler")

    event += handler
    event.clear()

    assert len(event) == 0
    fire(event, "tester", {"n": 1})
    assert calls == []


def test_cancel_event_stops_fanout_without_error(event_cls: EventClass) -> None:
    """CancelEvent stops the fan-out and does not propagate."""
    event = event_cls()
    calls: list[str] = []

    def stopper(caller: str, payload: PingPayload) -> None:
        raise CancelEvent

    def after(caller: str, payload: PingPayload) -> None:
        calls.append("after")

    event += stopper
    event += after

    fire(event, "tester", {"n": 1})

    assert calls == []


def test_subscribing_during_trigger_does_not_affect_current_fanout() -> None:
    """A subscriber added mid-fan-out is called from the next trigger on."""
    event: Event[str, PingPayload] = Event()
    order: list[str] = []

    def first(caller: str, payload: PingPayload) -> None:
        order.append("first")
        event.subscribe(late)

    def late(caller: str, payload: PingPayload) -> None:
        order.append("late")

    event += first

    event.trigger("tester", {"n": 1})
    assert order == ["first"]

    event.trigger("tester", {"n": 2})
    assert order == ["first", "first", "late"]


def test_unsubscribing_during_trigger_still_calls_this_pass() -> None:
    """A subscriber removed mid-fan-out still runs for the in-flight trigger."""
    event: Event[str, PingPayload] = Event()
    order: list[str] = []

    def first(caller: str, payload: PingPayload) -> None:
        order.append("first")
        event.unsubscribe(second)

    def second(caller: str, payload: PingPayload) -> None:
        order.append("second")

    event += first
    event += second

    event.trigger("tester", {"n": 1})
    assert order == ["first", "second"]

    event.trigger("tester", {"n": 2})
    assert order == ["first", "second", "first"]


def test_exception_aborts_fanout() -> None:
    """The first subscriber exception propagates; later subscribers are skipped."""
    event: Event[str, PingPayload] = Event()
    calls: list[str] = []

    def bad(caller: str, payload: PingPayload) -> None:
        raise ValueError("abort")

    def after(caller: str, payload: PingPayload) -> None:
        calls.append("after")

    event += bad
    event += after

    with pytest.raises(ValueError, match="abort"):
        event.trigger("tester", {"n": 1})

    assert calls == []


def test_sync_and_async_subscribers_run_in_order() -> None:
    """Sync and async subscribers interleave in subscription order."""
    event: AsyncEvent[str, PingPayload] = AsyncEvent()
    order: list[str] = []

    def sync_first(caller: str, payload: PingPayload) -> None:
        order.append("sync-first")

    async def async_middle(caller: str, payload: PingPayload) -> None:
        await asyncio.sleep(0)
        order.append("async-middle")

    def sync_last(caller: str, payload: PingPayload) -> None:
        order.append("sync-last")

    event += sync_first
    event += async_middle
    event += sync_last

    fire(event, "tester", {"n": 1})

    assert order == ["sync-first", "async-middle", "sync-last"]


def test_async_exception_aborts_fanout() -> None:
    """An exception from an async subscriber propagates; later ones are skipped."""
    event: AsyncEvent[str, PingPayload] = AsyncEvent()
    calls: list[str] = []

    async def bad(caller: str, payload: PingPayload) -> None:
        raise ValueError("abort")

    def after(caller: str, payload: PingPayload) -> None:
        calls.append("after")

    event += bad
    event += after

    with pytest.raises(ValueError, match="abort"):
        fire(event, "tester", {"n": 1})

    assert calls == []


def test_async_cancel_event_stops_fanout_without_error() -> None:
    """CancelEvent raised inside an awaited coroutine stops the fan-out."""
    event: AsyncEvent[str, PingPayload] = AsyncEvent()
    calls: list[str] = []

    async def stopper(caller: str, payload: PingPayload) -> None:
        await asyncio.sleep(0)
        raise CancelEvent

    def after(caller: str, payload: PingPayload) -> None:
        calls.append("after")

    event += stopper
    event += after

    fire(event, "tester", {"n": 1})

    assert calls == []


def test_sync_handler_returning_value_raises() -> None:
    """A sync subscriber returning a non-None value fails loudly on trigger."""
    event: AsyncEvent[str, PingPayload] = AsyncEvent()

    def sneaky(caller: str, payload: PingPayload) -> int:
        return 42

    event += sneaky  # type: ignore[arg-type]

    with pytest.raises(TypeError):
        fire(event, "tester", {"n": 1})
