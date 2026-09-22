from ducktape.mark import matrix, parametrize
from ducktape.tests.test import Test

NUM_TESTS = 5


class GoodArgsTest(Test):
    @parametrize(name="a", enabled=True, retries=3, ratio=0.5, tags=["x", "y"], opts={"k": "v"}, missing=None)
    def test_json_args(self, name, enabled, retries, ratio, tags, opts, missing):
        pass

    @matrix(replicas=[1, 3], mode=["async", "sync"])
    def test_matrix_args(self, replicas, mode):
        pass
