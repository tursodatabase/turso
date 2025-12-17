from __future__ import annotations

import threading
from queue import SimpleQueue
from threading import Thread
from typing import Any, Callable

STOP_RUNNING_SENTINEL = object()


class WorkItem:
    """A work item that can be cancelled and holds result/exception.

    Uses threading.Event for signaling, which can be created in sync context
    and safely set from the worker thread.
    """

    __slots__ = ("func", "event", "result", "exception", "cancelled")

    def __init__(self, func: Callable[[], Any]) -> None:
        self.func = func
        self.event = threading.Event()
        self.result: Any = None
        self.exception: BaseException | None = None
        self.cancelled: bool = False


class Worker(Thread):
    """
    Dedicated worker thread executing database operations sequentially.

    The worker consumes WorkItem objects from the unbounded SimpleQueue.
    It executes the callable, stores result/exception on the WorkItem,
    then signals completion via threading.Event.set().

    If work item returns STOP_RUNNING_SENTINEL value - it stops the execution
    (e.g. this can be used to stop worker when connection is about to close)
    """

    def __init__(
        self,
        queue: SimpleQueue[WorkItem],
    ) -> None:
        super().__init__(name="turso-async-worker", daemon=True)
        self._queue = queue

    def run(self) -> None:
        while True:
            item = self._queue.get()
            if item.cancelled:
                # Still consume but skip execution if already cancelled
                # Signal completion so any waiter doesn't block forever
                item.event.set()
                continue
            try:
                result = item.func()
                if result is STOP_RUNNING_SENTINEL:
                    # Signal completion before breaking
                    item.result = None
                    item.event.set()
                    break
            except BaseException as e:
                item.exception = e
                item.event.set()
            else:
                item.result = result
                item.event.set()
