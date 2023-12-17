import time
import contextlib
import pytest
from unittest.mock import patch

import dramatiq
from dramatiq.common import current_millis


@contextlib.contextmanager
def create_worker(*args, **kwargs):
    try:
        worker = dramatiq.Worker(*args, **kwargs)
        worker.start()
        yield worker
    finally:
        worker.stop()


def get_consumer(worker: dramatiq.Worker) -> dramatiq.Consumer:
    consumer = list(worker.consumers.values())[0].consumer
    assert consumer
    return consumer


def test_work(broker, worker):
    # Given that I have a database
    database = {}

    # And an actor that can write data to that database
    @dramatiq.actor()
    def put(key, value):
        database[key] = value

    # If I send that actor many async messages
    for i in range(100):
        assert put.send("key-%s" % i, i)

    # And I give the workers time to process the messages
    broker.join(put.queue_name)
    worker.join()

    # I expect the database to be populated
    assert len(database) == 100


def test_work_in_multiple_queue(broker, worker):
    # Given that I have a database
    default_db = {}
    other_db = {}

    # And an actor that can write data to that database
    @dramatiq.actor(queue_name="default")
    def put(key, value):
        default_db[key] = value

    @dramatiq.actor(queue_name="other")
    def put_other(key, value):
        other_db[key] = value

    # If I send that actor many async messages
    for i in range(50):
        assert put.send("key-%s" % i, i)
        assert put_other.send("key-%s" % i, i)

    # And I give the workers time to process the messages
    broker.join(put.queue_name)
    broker.join(put_other.queue_name)
    worker.join()

    # I expect the database to be populated
    assert len(default_db) == 50
    assert len(other_db) == 50


def test_worker_can_be_sent_messages(broker, worker):
    # Given that I have a database
    database = {}

    # And an actor that can write data to that database
    @dramatiq.actor()
    def put(key, value):
        database[key] = value

    # If I send that actor many async messages
    for i in range(100):
        assert put.send("key-%s" % i, i)

    # And I give the workers time to process the messages
    broker.join(put.queue_name)
    worker.join()

    # I expect the database to be populated
    assert len(database) == 100


def test_retry_with_backoff_on_failure(broker, worker):
    # Given that I have a database
    failure_time, success_time = None, None

    # And an actor that fails the first time it's called
    @dramatiq.actor(min_backoff=1000, max_backoff=5000, max_retries=1)
    def do_work():
        nonlocal failure_time, success_time
        if not failure_time:
            failure_time = current_millis()
            raise RuntimeError("First failure.")
        else:
            success_time = current_millis()

    # If I send it a message
    do_work.send()

    # Then join on the queue
    broker.join(do_work.queue_name)
    worker.join()

    # I expect backoff time to have passed between success and failure
    # + the worker idle timeout as padding in case the worker is long polling
    assert 500 <= success_time - failure_time <= 2500 + worker.worker_timeout


def test_worker_can_retry_multiple_times(broker, worker):
    # Given that I have a database
    attempts = []

    # And an actor that fails 3 times then succeeds
    @dramatiq.actor(min_backoff=1000, max_backoff=1000)
    def do_work():
        attempts.append(1)
        if sum(attempts) < 4:
            raise RuntimeError("Failure #%s" % sum(attempts))

    # If I send it a message
    do_work.send()

    # Then join on the queue
    broker.join(do_work.queue_name)
    worker.join()

    # I expect it to have been attempted 4 times
    assert sum(attempts) == 4


def test_worker_can_have_their_messages_delayed(broker, worker):
    # Given that I have a database
    start_time, run_time = current_millis(), None

    # And an actor that records the time it ran
    @dramatiq.actor()
    def record():
        nonlocal run_time
        run_time = current_millis()

    # If I send it a delayed message
    record.send_with_options(delay=1000)

    # Then join on the queue
    broker.join(record.queue_name)
    worker.join()

    # I expect that message to have been processed at least delayed milliseconds later
    assert run_time - start_time >= 1000


def test_worker_can_delay_messages_independent_of_each_other(broker):
    # Given that I have a database
    results = []

    # And an actor that appends a number to the database
    @dramatiq.actor()
    def append(x):
        results.append(x)

    # When I pause the worker
    with create_worker(broker, worker_timeout=100, worker_threads=1) as worker:
        worker.pause()

        # And I send it a delayed message
        append.send_with_options(args=(1,), delay=2000)

        # And then another delayed message with a smaller delay
        append.send_with_options(args=(2,), delay=1000)

        # Then resume the worker and join on the queue
        worker.resume()
        broker.join(append.queue_name)
        worker.join()

        # I expect the latter message to have been run first
        assert results == [2, 1]


def test_messages_can_be_dead_lettered(broker, worker):
    # Given that I have an actor that always fails
    @dramatiq.actor(max_retries=0)
    def do_work():
        raise RuntimeError("failed")

    # If I send it a message
    do_work.send()

    consumer = get_consumer(worker)
    with patch.object(consumer, "nack", side_effect=consumer.nack) as nack_mock:
        # And then join on its queue
        broker.join(do_work.queue_name)
        worker.join()

        # I expect it to end up in the dead letter queue
        nack_mock.assert_called_once()


def test_messages_belonging_to_missing_actors_are_rejected(broker, worker):
    # Given that I have a broker without actors
    # If I send it a message
    message = dramatiq.Message(
        queue_name="some-queue",
        actor_name="some-actor",
        args=(),
        kwargs={},
        options={},
    )
    broker.declare_queue("some-queue")
    message = broker.enqueue(message)

    consumer = get_consumer(worker)
    with patch.object(consumer, "nack", side_effect=consumer.nack) as nack_mock:
        # Then join on the queue
        broker.join("some-queue")
        worker.join()
        # I expect the message to end up on the dead letter queue
        nack_mock.assert_called_once()


def test_requeues_unhandled_messages_on_shutdown(broker):
    # Given that I have an actor that takes its time
    finished = 0

    @dramatiq.actor
    def do_work():
        nonlocal finished
        time.sleep(1)
        finished += 1

    # If I send it two messages
    do_work.send()
    do_work.send()

    # Then start a worker and subsequently shut it down
    with create_worker(broker, worker_threads=1):
        time.sleep(0.25)

    # I expect it to have processed one of the messages and re-enqueued the other
    assert finished == 1

    with create_worker(broker, worker_threads=1):
        time.sleep(0.25)
    assert finished == 2


def test_broker_can_join_with_timeout(broker, worker):
    # Given that I have an actor that takes a long time to run
    @dramatiq.actor
    def do_work():
        time.sleep(1)

    # When I send that actor a message
    do_work.send()

    # And join on its queue with a timeout
    # Then I expect a QueueJoinTimeout to be raised
    with pytest.raises(dramatiq.QueueJoinTimeout):
        broker.join(do_work.queue_name, timeout=500)


def test_broker_can_flush_queues(broker):
    # Given that I have an actor
    @dramatiq.actor
    def do_work():
        pass

    # When I send that actor a message
    do_work.send()

    # And then tell the broker to flush all queues
    broker.flush_all()

    # And then join on the actors's queue
    # Then it should join immediately
    assert broker.join(do_work.queue_name, timeout=200) is None
