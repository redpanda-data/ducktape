import threading

from ducktape.mark.resource import cluster
from ducktape.services.background_thread import BackgroundThreadService
from ducktape.tests.test import Test


class WorkerService(BackgroundThreadService):
    """Service whose worker runs until stop_node is called."""

    free_worker_join_timeout_sec = .5

    def __init__(self, context):
        super(WorkerService, self).__init__(context, num_nodes=1)
        self.stopped = threading.Event()

    def start_node(self, node):
        # start() calls stop_node before start_node
        self.stopped.clear()
        super(WorkerService, self).start_node(node)

    def _worker(self, idx, node):
        self.stopped.wait()

    def stop_node(self, node):
        self.stopped.set()


class UnstoppableWorkerService(WorkerService):
    """Service whose stop_node does not end its worker, like a worker reading the output of a remote
    process that stop and clean failed to find."""

    def stop_node(self, node):
        pass


class LeakedWorkerTest(Test):

    @cluster(num_nodes=1)
    def test_stopped_worker(self):
        service = WorkerService(self.test_context)
        service.start()
        service.stop()

    @cluster(num_nodes=1)
    def test_worker_survives_teardown(self):
        UnstoppableWorkerService(self.test_context).start()

    @cluster(num_nodes=1)
    def test_free_running_worker(self):
        service = WorkerService(self.test_context)
        service.start()
        service.free()
