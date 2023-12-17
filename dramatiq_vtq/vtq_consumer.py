from collections.abc import Iterable

import dramatiq.logging
from dramatiq import Message, MessageProxy
from vtq import TaskQueue
from . import message as message_mod


class VtqConsumer(dramatiq.Consumer):
    def __init__(self, task_queue: TaskQueue, timeout: int):
        self.task_queue = task_queue
        self.timeout = timeout
        self.logger = dramatiq.logging.get_logger(__name__, type(self))

    def ack(self, message: Message):
        self.logger.debug(f"ack {message.message_id}")

        # A better solution is to add a message.retrying condition in _ConsumerThread.post_process_message, and then call Consumer.retry method instead of the Consumer.ack method. Then the Retries Middleware has to set the message to be retrying instead of enqueue a new message.
        if message_mod.is_retrying(message):
            self.logger.info(f"Skip acking {message.message_id} as it is retrying")
            self.retry(message, delay=message_mod.pop_retry_delay_millis(message))
            return

        if not self.task_queue.ack(message_mod.get_msg_id(message)):
            raise Exception(f"[VtqConsumer] ack error for {message.message_id}")

    def nack(self, message: Message):
        self.logger.debug(f"nack {message.message_id}")
        error_message = self._get_error_message(message)
        if not self.task_queue.nack(message_mod.get_msg_id(message), error_message):
            raise Exception(f"[VtqConsumer] nack error for {message.message_id}")

    def retry(self, message: Message, delay=0):
        self.logger.debug(f"retry {message.message_id}")
        rv = self.task_queue.retry(
            task_id=message_mod.pop_msg_id(message),
            delay_millis=delay,
            error_message=self._get_error_message(message),
        )
        if not rv:
            raise Exception(f"[VtqConsumer] Retry error for {message.message_id}")

    def _get_error_message(self, message: Message) -> str:
        if not isinstance(message, MessageProxy):
            self.logger.warning(f"Not a MessageProxy {message.message_id}")
            return ""

        exc = message._exception
        if not exc:
            self.logger.warning(f"No exception on the message {message.message_id}")
            return ""

        traceback = message.options.get("traceback", "")
        if traceback:
            return f"{exc}\n{traceback}"

        return str(exc)

    def requeue(self, messages: Iterable[Message]):
        self.logger.debug("requeue messages")
        error_count = 0
        for message in messages:
            if not self.task_queue.requeue(message_mod.get_msg_id(message)):
                error_count += 1
                self.logger.error(
                    f"[VtqConsumer] requeue error for {message.message_id}"
                )
        if error_count:
            raise Exception(f"[VtqConsumer] requeue error with {error_count} messages")

    def __next__(self) -> MessageProxy | None:
        tasks = self.task_queue.receive(wait_time_seconds=self.timeout / 1000)
        if not tasks:
            return
        task = tasks[0]
        message = Message.decode(task.data)
        message_mod.set_msg_id(message, task.id)
        message_mod.set_retries(message, retries=task.meta.retries)
        self.logger.debug(f"receive {message.message_id}")
        return MessageProxy(message)
