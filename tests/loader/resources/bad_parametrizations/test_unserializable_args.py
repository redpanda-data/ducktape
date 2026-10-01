import enum

from ducktape.mark import parametrize
from ducktape.tests.test import Test


class Flavor(enum.Enum):
    VANILLA = 1


class UnserializableArgsTest(Test):
    @parametrize(flavor=Flavor.VANILLA)
    def test_enum_arg(self, flavor):
        pass
