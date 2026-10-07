"""A minimal, typed event primitive."""

from collections.abc import Awaitable, Callable
from typing import Generic, Self, TypeVar

__all__ = ["AsyncEvent", "CancelEvent", "Event"]

S = TypeVar("S")
K = TypeVar("K")


class CancelEvent(Exception):
    """Raise in a subscriber to stop the fan-out without propagating an error."""


class Event(Generic[S, K]):
    """Fan one ``trigger`` call out to every subscribed callable.

    Subscribers are callables of the form ``f(caller: S, payload: K)``.
    ``S`` is the sender's type and ``K`` is the payload type (usually a
    ``TypedDict``), so the type checker verifies the sender and every payload
    key at every call site.

    For an event with no meaningful sender (a timer, a global), declare it as
    ``Event[None, Payload]`` and trigger with ``event(None, payload)``.

    Raising :class:`CancelEvent` in a subscriber stops the fan-out: the
    remaining subscribers are not called and no error reaches the caller.

    ``event += callback`` and ``event -= callback`` are aliases for
    :meth:`subscribe` and :meth:`unsubscribe`.
    """

    __slots__ = ("_callbacks",)

    def __init__(self) -> None:
        """Create an event with no subscribers."""
        self._callbacks: list[Callable[[S, K], None]] = []

    def __call__(self, caller: S, payload: K) -> None:
        """Alias for :meth:`trigger`: ``event(caller, payload)``."""
        self.trigger(caller, payload)

    def __len__(self) -> int:
        """Return the number of subscribers."""
        return len(self._callbacks)

    def __repr__(self) -> str:
        """Return a human-readable representation of the event."""
        return f"{type(self).__name__}(subscribers={len(self._callbacks)})"

    def subscribe(self, callback: Callable[[S, K], None]) -> None:
        """Register ``callback``; a no-op if it is already subscribed."""
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def unsubscribe(self, callback: Callable[[S, K], None]) -> None:
        """Remove ``callback``; a no-op if it was not subscribed."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def __iadd__(self, callback: Callable[[S, K], None]) -> Self:
        """``event += callback``: alias for :meth:`subscribe`."""
        self.subscribe(callback)
        return self

    def __isub__(self, callback: Callable[[S, K], None]) -> Self:
        """``event -= callback``: alias for :meth:`unsubscribe`."""
        self.unsubscribe(callback)
        return self

    def trigger(self, caller: S, payload: K) -> None:
        """Invoke every subscriber with ``caller`` and ``payload``."""
        for callback in list(self._callbacks):
            try:
                callback(caller, payload)
            except CancelEvent:
                break

    def clear(self) -> None:
        """Remove all subscribers."""
        self._callbacks.clear()


class AsyncEvent(Generic[S, K]):
    """Async-aware variant of :class:`Event`; subscribers may be sync or async.

    ``trigger`` awaits subscribers **in subscription order**, so the
    ordering contract of :class:`Event` is preserved: a subscriber never
    observes side effects from a later one. Raising
    :class:`CancelEvent` stops the fan-out gracefully; any other
    exception aborts it and propagates to the caller.

    Triggering requires a running event loop: call sites must be
    coroutines (``await event(caller, payload)``).

    ``event += callback`` and ``event -= callback`` are aliases for
    :meth:`subscribe` and :meth:`unsubscribe`.
    """

    __slots__ = ("_callbacks",)

    def __init__(self) -> None:
        """Create an async event with no subscribers."""
        self._callbacks: list[
            Callable[[S, K], None] | Callable[[S, K], Awaitable[None]]
        ] = []

    def __len__(self) -> int:
        """Return the number of subscribers."""
        return len(self._callbacks)

    def __repr__(self) -> str:
        """Return a human-readable representation of the event."""
        return f"{type(self).__name__}(subscribers={len(self._callbacks)})"

    def subscribe(
        self,
        callback: Callable[[S, K], None] | Callable[[S, K], Awaitable[None]],
    ) -> None:
        """Register ``callback``; a no-op if it is already subscribed."""
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def unsubscribe(
        self,
        callback: Callable[[S, K], None] | Callable[[S, K], Awaitable[None]],
    ) -> None:
        """Remove ``callback``; a no-op if it was not subscribed."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def __iadd__(
        self,
        callback: Callable[[S, K], None] | Callable[[S, K], Awaitable[None]],
    ) -> Self:
        """``event += callback``: alias for :meth:`subscribe`."""
        self.subscribe(callback)
        return self

    def __isub__(
        self,
        callback: Callable[[S, K], None] | Callable[[S, K], Awaitable[None]],
    ) -> Self:
        """``event -= callback``: alias for :meth:`unsubscribe`."""
        self.unsubscribe(callback)
        return self

    async def __call__(self, caller: S, payload: K) -> None:
        """Alias for :meth:`trigger`: ``await event(caller, payload)``."""
        await self.trigger(caller, payload)

    async def trigger(self, caller: S, payload: K) -> None:
        """Invoke every subscriber in order, awaiting each one."""
        for callback in list(self._callbacks):
            try:
                outcome = callback(caller, payload)
                if outcome is not None:
                    await outcome
            except CancelEvent:
                break

    def clear(self) -> None:
        """Remove all subscribers."""
        self._callbacks.clear()
