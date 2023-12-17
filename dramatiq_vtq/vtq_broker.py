import threading
import time
from collections.abc import Callable

import dramatiq
import vtq
import vtq.workspace
from vtq import TaskQueue, Workspace

from .vtq_consumer import VtqConsumer
from . import message as message_mod


class VtqBroker(dramatiq.Broker):
    def __init__(
        self,
        middleware=None,
        *,
        workspace_factory: Callable[[str], Workspace] | None = None,
    ):
        self._workspace_factory = workspace_factory or vtq.workspace.DefaultWorkspace

        self._workspaces: dict[str, Workspace] = {}
        self._ws_creation_lock = threading.Lock()

        super().__init__(middleware)

    def _add_ws(self, name: str):
        ws = self._workspace_factory(name)
        ws.init()
        self._workspaces[name] = ws

    def _get_task_queue(self, queue_name) -> TaskQueue:
        self.declare_queue(queue_name)
        return self._workspaces[queue_name].coordinator

    def _declare_queue(self, queue_name):
        self.emit_before("declare_queue", queue_name)
        self._add_ws(queue_name)
        self.emit_after("declare_queue", queue_name)
        # There VTQ support can add delay task but it behave just like a normal queue, so we don't have to emit `declare_delay_queue` mesage.

    def declare_queue(self, queue_name):
        if queue_name not in self._workspaces:
            with self._ws_creation_lock:
                if queue_name not in self._workspaces:
                    self._declare_queue(queue_name)

    def get_declared_queues(self) -> set[str]:
        return set(self._workspaces.keys())

    def get_declared_delay_queues(self) -> set[str]:
        return set()

    def consume(self, queue_name, prefetch=1, timeout=30000) -> dramatiq.Consumer:
        """The consume method is called in multi-threading situation"""
        if prefetch > 1:
            self.logger.warning(
                "prefetch parameter doesn't work in VtqBroker right now."
            )
        task_queue = self._get_task_queue(queue_name)
        return VtqConsumer(task_queue, timeout)

    def enqueue(self, message: dramatiq.Message, *, delay=None, **kwargs):
        task_queue = self._get_task_queue(message.queue_name)

        # retry the message instead of enqueue a new message and ack current message
        if isinstance(message, dramatiq.MessageProxy) and message._exception:
            # A flag to prevent acking the current message.
            message_mod.set_is_retrying(message, delay_millis=delay)
            return

        self.emit_before("enqueue", message, delay)
        task_queue.enqueue(task_data=message.encode(), delay_millis=delay, **kwargs)
        self.emit_after("enqueue", message, delay)
        return message

    def close(self):
        self.logger.debug("Closing workspaces...")
        for ws in self._workspaces.values():
            ws.close()
        self.logger.debug("Workspaces closed...")

    def flush(self, queue_name):
        """Used in the test only"""
        ws = self._workspaces[queue_name]
        ws.flush_all()

    def flush_all(self):
        """Used in the test only"""
        for queue_name in self._workspaces:
            self.flush(queue_name)

    def join(self, queue_name, *, interval=100, timeout=None):
        """Used in the test only"""
        deadline = timeout and time.monotonic() + timeout / 1000
        task_queue = self._get_task_queue(queue_name)
        assert task_queue is not None
        while True:
            if deadline and time.monotonic() >= deadline:
                raise dramatiq.QueueJoinTimeout(queue_name)

            size = len(task_queue)
            self.logger.debug(
                f"VTQ workspace {queue_name} uncompleted task size {size}"
            )

            if size == 0:
                return

            time.sleep(interval / 1000)
