from typing import Callable
import time
import threading
import dramatiq
from dramatiq import Message, MessageProxy
import vtq
from vtq import Workspace, TaskQueue


class VtqBroker(dramatiq.Broker):
    def __init__(
        self,
        middleware=None,
        *,
        workspace_factory: Callable[[str], Workspace] | None = None,
    ):
        super().__init__(middleware)

        self._workspace_factory = workspace_factory or vtq.WorkspaceFactory

        self._workspaces: dict[str, TaskQueue] = {}
        self._ws_creation_lock = threading.Lock()

    def _add_ws(self, name: str):
        ws = self._workspace_factory(name)
        self._workspaces[name] = ws.coordinator

    def _get_task_queue(self, queue_name) -> TaskQueue:
        self.declare_queue(queue_name)
        return self._workspaces[queue_name]

    def consume(self, queue_name, prefetch=1, timeout=30000) -> dramatiq.Consumer:
        """The consume method is called in multi-threading situation"""
        if prefetch > 1:
            raise NotImplementedError("prefetch hasn't been implemented")
        task_queue = self._get_task_queue(queue_name)
        return _VtqConsumer(self, task_queue, timeout)

    def _declare_queue(self, queue_name):
        self.emit_before("declare_queue", queue_name)
        self._add_ws(queue_name)
        self.emit_after("declare_queue", queue_name)
        self.emit_after("declare_delay_queue", queue_name)

    def declare_queue(self, queue_name):
        if queue_name not in self._workspaces:
            with self._ws_creation_lock:
                if queue_name not in self._workspaces:
                    self._declare_queue(queue_name)

    def enqueue(self, message: dramatiq.Message, *, delay=None, **kwargs):
        queue_name = message.queue_name
        task_queue = self._get_task_queue(queue_name)
        self.emit_before("enqueue", message, delay)
        task_queue.enqueue(task_data=message.encode(), delay_millis=delay, **kwargs)
        self.emit_after("enqueue", message, delay)
        return message

    def get_declared_queues(self) -> set[str]:
        return set(self._workspaces.keys())

    def get_declared_delay_queues(self) -> set[str]:
        return self.get_declared_queues()

    def join(self, queue_name, *, interval=100, timeout=None):
        """Used in the test only"""
        deadline = timeout and time.monotonic() + timeout / 1000
        task_queue = self._get_task_queue(queue_name)
        assert task_queue
        while True:
            if deadline and time.monotonic() >= deadline:
                raise dramatiq.QueueJoinTimeout(queue_name)

            size = len(task_queue)

            if size == 0:
                return

            time.sleep(interval / 1000)


class _VtqConsumer(dramatiq.Consumer):
    _MSG_ID_KEY = "vtq_msg_id"

    def __init__(self, broker: VtqBroker, task_queue: TaskQueue, timeout: int):
        self.broker = broker
        self.task_queue = task_queue
        self.timeout = timeout

    def _get_msg_id(self, message: Message):
        assert message.options[self._MSG_ID_KEY]
        return message.options[self._MSG_ID_KEY]

    def ack(self, message: Message):
        if not self.task_queue.ack(self._get_msg_id(message)):
            raise Exception(f"[VtqConsumer] ack error for {message.message_id}")

    def nack(self, message: Message):
        if not self.task_queue.nack(self._get_msg_id(message)):
            raise Exception(f"[VtqConsumer] nack error for {message.message_id}")

    def __next__(self) -> MessageProxy | None:
        tasks = self.task_queue.receive(wait_time_seconds=self.timeout / 1000)
        if not tasks:
            return
        task = tasks[0]
        message = Message.decode(task.data)
        message.options[self._MSG_ID_KEY] = task.id
        return MessageProxy(message)
