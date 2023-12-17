import pytest
import dramatiq
import queue
from dramatiq_vtq import vtq_broker
from vtq.workspace import MemoryWorkspace


@pytest.fixture()
def debug_logger():
    import logging

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("dramatiq").setLevel(logging.DEBUG)
    logger = logging.getLogger("dramatiq-vtq.test")
    logger.setLevel(logging.DEBUG)
    yield logger
    # TODO: restore original handler and levels


@pytest.fixture()
def broker():
    broker = vtq_broker.VtqBroker(workspace_factory=MemoryWorkspace)
    broker.flush_all()
    broker.emit_after("process_boot")
    dramatiq.set_broker(broker)
    yield broker
    broker.close()
    broker.flush_all()


@pytest.fixture(params=[1, 2], ids=["1 worker thread", "2 worker threads"])
def worker(broker, request):
    worker = dramatiq.Worker(broker, worker_threads=request.param)
    worker.work_queue = queue.PriorityQueue(maxsize=100)
    worker.start()
    yield worker
    worker.stop()


@pytest.fixture()
def create_worker(broker):
    workers = []

    def _create_worker(**kwargs):
        worker = dramatiq.Worker(broker, **kwargs)
        worker.work_queue = queue.PriorityQueue(maxsize=100)
        workers.append(worker)
        worker.start()
        return worker

    yield _create_worker

    for worker in workers:
        worker.stop()
