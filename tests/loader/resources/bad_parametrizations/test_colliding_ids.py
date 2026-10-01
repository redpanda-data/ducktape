from ducktape.mark import parametrize
from ducktape.tests.test import Test


class CollidingIdsTest(Test):
    # both escape to test id "path=a.b"
    @parametrize(path="a/b")
    @parametrize(path="a.b")
    def test_punctuated_arg(self, path):
        pass
