from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.services.service import Service
from ducktape.tests.test import Test


class UncleanService(Service):
    """Service whose clean phase always fails, simulating leaked resources on its nodes."""

    def clean_node(self, node, **kwargs):
        raise RuntimeError("simulated clean failure")


class UncleanTeardownTest(Test):

    @cluster(num_nodes=1)
    @matrix(x=[_ for _ in range(2)])
    def test_body_passes(self, x):
        """The test body itself always passes; only service clean-up fails."""
        UncleanService(self.test_context, num_nodes=1)
