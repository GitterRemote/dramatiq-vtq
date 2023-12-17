from dramatiq import Message

_MSG_ID_KEY = "vtq_msg_id"
_IS_RETRYING_KEY = "vtq_retrying_msg"


def get_msg_id(message: Message) -> str:
    assert message.options[_MSG_ID_KEY]
    return message.options[_MSG_ID_KEY]


def set_msg_id(message: Message, message_id: str):
    message.options[_MSG_ID_KEY] = message_id


def pop_msg_id(message: Message) -> str:
    return message.options.pop(_MSG_ID_KEY)


def set_is_retrying(message: Message, delay_millis: int):
    assert message.options["retries"] >= 1
    message.options[_IS_RETRYING_KEY] = delay_millis


def is_retrying(message: Message) -> bool:
    return _IS_RETRYING_KEY in message.options


def pop_retry_delay_millis(message: Message) -> int:
    return message.options.pop(_IS_RETRYING_KEY)


def set_retries(message: Message, retries: int):
    assert "retries" not in message.options
    message.options["retries"] = retries
