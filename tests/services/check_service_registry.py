from unittest.mock import MagicMock

from ducktape.services.service_registry import ServiceRegistry


class CheckServiceRegistry(object):

    def _fake_service(self, name):
        service = MagicMock(nodes=[])
        service.who_am_i.return_value = name
        service.error = None
        return service

    def check_clean_all_records_clean_failures(self):
        """clean_all should keep cleaning after a failure and record every failure both in
        clean_errors and on the failed service's error attribute."""
        registry = ServiceRegistry()
        bad = self._fake_service("BadService-1")
        bad.clean.side_effect = RuntimeError("boom")
        good = self._fake_service("GoodService-1")
        registry.append(bad)
        registry.append(good)

        registry.clean_all()

        assert good.clean.called
        assert len(registry.clean_errors) == 1
        assert "BadService-1" in registry.clean_errors[0]
        assert "boom" in registry.clean_errors[0]
        assert "BadService-1" in registry.errors()
        assert not good.error

    def check_free_all_resets_clean_errors(self):
        registry = ServiceRegistry()
        bad = self._fake_service("BadService-1")
        bad.clean.side_effect = RuntimeError("boom")
        registry.append(bad)

        registry.clean_all()
        assert registry.clean_errors

        registry.free_all()
        assert not registry.clean_errors
