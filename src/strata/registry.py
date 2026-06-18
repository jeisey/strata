"""The handler registry -- the dynamic-dispatch half of Layer 3.

The routes table answers "which handler should run for this signature?"; the
registry answers "what code is that handler?".  Together they reproduce the
classic pattern of a lookup table naming a stored proc plus dynamic SQL to run
it -- here, a name resolving to a Python callable that is then invoked.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from .exceptions import NoHandlerError

if TYPE_CHECKING:  # avoid an import cycle; these are only used for type hints
    from .output import OutputWriter
    from .schema import StructuredBlob

# A handler takes a structured (table-like) blob and an output writer.
HandlerFn = Callable[["StructuredBlob", "OutputWriter"], None]


class HandlerRegistry:
    """A name -> handler-callable mapping."""

    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFn] = {}

    def register(
        self, name: str, fn: Optional[HandlerFn] = None
    ) -> Callable[[HandlerFn], HandlerFn]:
        """Register a handler.

        Works both as a direct call and as a decorator::

            registry.register("acme_prices_v1", handle_acme)

            @registry.register("acme_prices_v1")
            def handle_acme(blob, out): ...
        """
        if fn is not None:
            self._handlers[name] = fn
            return fn

        def decorator(func: HandlerFn) -> HandlerFn:
            self._handlers[name] = func
            return func

        return decorator

    def get(self, name: str) -> HandlerFn:
        try:
            return self._handlers[name]
        except KeyError:
            raise NoHandlerError(
                f"no handler named '{name}' is registered "
                f"(known: {', '.join(sorted(self._handlers)) or '(none)'})"
            ) from None

    def names(self) -> list[str]:
        return sorted(self._handlers)

    def __contains__(self, name: object) -> bool:
        return name in self._handlers

    def __len__(self) -> int:
        return len(self._handlers)
