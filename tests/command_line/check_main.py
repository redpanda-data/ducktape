# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.command_line.main import collected_test_record
from ducktape.command_line.main import get_user_defined_globals
from ducktape.command_line.main import main
from ducktape.command_line.main import setup_results_directory
from ducktape.command_line.main import update_latest_symlink
from ducktape.tests.loader import TestLoader

import tests.ducktape_mock

import json
import os
import os.path
import pickle
import pytest
import sys
import tempfile

from mock import Mock


LOADER_TEST_DIRECTORY = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "loader", "resources", "loader_test_directory"))
TEST_FILES = [os.path.join(LOADER_TEST_DIRECTORY, name) for name in ("test_a.py", "test_decorated.py")]


class CheckCollectedTestRecord(object):
    def check_symbol_loads_only_its_test(self):
        good_args_file = os.path.join(LOADER_TEST_DIRECTORY, "..", "bad_parametrizations", "test_good_args.py")
        loader = TestLoader(tests.ducktape_mock.session_context(), logger=Mock())
        collected = loader.load(TEST_FILES + [good_args_file])
        assert any(test.injected_args is None for test in collected)
        assert any(test.injected_args == {} for test in collected)
        assert any(test.injected_args for test in collected)

        for test in collected:
            reloaded = loader.load([collected_test_record(test)["symbol"]])
            assert [t.test_id for t in reloaded] == [test.test_id]


class CheckCollectOutput(object):
    @pytest.fixture(autouse=True)
    def in_tmp_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self.monkeypatch = monkeypatch

    def run_main(self, *args):
        self.monkeypatch.setattr(sys, "argv", ["ducktape", "--config-file", "no-such-config", *args])
        with pytest.raises(SystemExit) as e:
            main()
        return e.value.code

    def check_json(self, capsys):
        assert self.run_main("--collect-only", "--collect-output", "collected.json", *TEST_FILES) == 0

        with open("collected.json") as fp:
            records = json.load(fp)["tests"]
        assert len(records) == 19
        assert capsys.readouterr().out.endswith("Collected 19 tests into collected.json\n")

        record = next(r for r in records if r["test_id"].endswith("TestParametrized.test_thing.x=1.y=2"))
        assert record == {
            "symbol": os.path.join(LOADER_TEST_DIRECTORY, "test_decorated.py")
            + '::TestParametrized.test_thing@{"x":1,"y":2}',
            "test_id": record["test_id"],
            "module_name": record["module_name"],
            "cls_name": "TestParametrized",
            "function_name": "test_thing",
            "file_name": os.path.join(LOADER_TEST_DIRECTORY, "test_decorated.py"),
            "injected_args": {"x": 1, "y": 2},
            "expected_num_nodes": 0,
            "ignore": False,
        }

    def check_text_matches_stdout(self, capsys):
        assert self.run_main("--collect-only", *TEST_FILES) == 0
        stdout = capsys.readouterr().out

        assert self.run_main("--collect-only", "--collect-output", "collected", *TEST_FILES) == 0
        with open("collected") as fp:
            assert fp.read() == stdout
        assert stdout.startswith("Collected 19 tests:\n")

    @pytest.mark.parametrize(["args", "message"], [
        pytest.param(["--collect-output", "collected.json"], "requires --collect-only", id="without collect-only"),
        pytest.param(["--collect-only", "--collect-output", "collected.jsn"], "must end in .json", id="bad suffix"),
    ])
    def check_rejects_before_creating_results(self, capsys, args, message):
        assert self.run_main(*args, *TEST_FILES) == 1
        assert message in capsys.readouterr().out
        assert not os.path.exists("results")


class CheckSetupResultsDirectory(object):
    def setup_method(self, _):
        self.results_root = tempfile.mkdtemp()
        self.results_dir = os.path.join(self.results_root, "results_directory")
        self.latest_symlink = os.path.join(self.results_root, "latest")

    def validate_directories(self):
        """Validate existence of results directory and correct symlink"""
        assert os.path.exists(self.results_dir)
        assert os.path.exists(self.latest_symlink)
        assert os.path.islink(self.latest_symlink)

        # check symlink points to correct location
        assert os.path.realpath(self.latest_symlink) == os.path.realpath(self.results_dir)

    def check_creation(self):
        """Check results and symlink from scratch"""
        assert not os.path.exists(self.results_dir)
        setup_results_directory(self.results_dir)
        update_latest_symlink(self.results_root, self.results_dir)
        self.validate_directories()

    def check_symlink(self):
        """Check "latest" symlink behavior"""

        # if symlink already exists
        old_results = os.path.join(self.results_root, "OLD")
        os.mkdir(old_results)
        os.symlink(old_results, self.latest_symlink)
        assert os.path.islink(self.latest_symlink) and os.path.exists(self.latest_symlink)

        setup_results_directory(self.results_dir)
        update_latest_symlink(self.results_root, self.results_dir)
        self.validate_directories()

        # Try again if symlink exists and points to nothing
        os.rmdir(self.results_dir)
        assert os.path.islink(self.latest_symlink) and not os.path.exists(self.latest_symlink)
        setup_results_directory(self.results_dir)
        update_latest_symlink(self.results_root, self.results_dir)
        self.validate_directories()


globals_json = """
{
    "x": 200
}
"""

invalid_globals_json = """
{
    can't parse this!: ?right?
}
"""

valid_json_not_dict = """
[
    {
        "x": 200,
        "y": 300
    }
]
"""


class CheckUserDefinedGlobals(object):
    """Tests for the helper method which parses in user defined globals option"""

    def check_immutable(self):
        """Expect the user defined dict object to be immutable."""
        global_dict = get_user_defined_globals(globals_json)

        with pytest.raises(NotImplementedError):
            global_dict["x"] = -1

        with pytest.raises(NotImplementedError):
            global_dict["y"] = 3

    def check_pickleable(self):
        """Expect the user defined dict object to be pickleable"""
        globals_dict = get_user_defined_globals(globals_json)

        assert globals_dict  # Need to test non-empty dict, to ensure py3 compatibility
        assert pickle.loads(pickle.dumps(globals_dict)) == globals_dict

    def check_parseable_json_string(self):
        """Check if globals_json is parseable as JSON, we get back a dictionary view of parsed JSON."""
        globals_dict = get_user_defined_globals(globals_json)
        assert globals_dict == json.loads(globals_json)

    def check_unparseable(self):
        """If globals string is not a path to a file, and not parseable as JSON we want to raise a ValueError
        """
        with pytest.raises(ValueError):
            get_user_defined_globals(invalid_globals_json)

    def check_parse_from_file(self):
        """Validate that, given a filename of a file containing valid JSON, we correctly parse the file contents."""
        _, fname = tempfile.mkstemp()
        try:
            with open(fname, "w") as fh:
                fh.write(globals_json)

            global_dict = get_user_defined_globals(fname)
            assert global_dict == json.loads(globals_json)
            assert global_dict["x"] == 200
        finally:
            os.remove(fname)

    def check_bad_parse_from_file(self):
        """Validate behavior when given file containing invalid JSON"""
        _, fname = tempfile.mkstemp()
        try:
            with open(fname, "w") as fh:
                # Write invalid JSON
                fh.write(invalid_globals_json)

            with pytest.raises(ValueError):
                get_user_defined_globals(fname)

        finally:
            os.remove(fname)

    def check_non_dict(self):
        """Valid JSON which does not parse as a dict should raise a ValueError"""

        # Should be able to parse this as JSON
        json.loads(valid_json_not_dict)

        with pytest.raises(ValueError):
            get_user_defined_globals(valid_json_not_dict)

    def check_non_dict_from_file(self):
        """Validate behavior when given file containing valid JSON which does not parse as a dict"""
        _, fname = tempfile.mkstemp()
        try:
            with open(fname, "w") as fh:
                # Write valid JSON which does not parse as a dict
                fh.write(valid_json_not_dict)

            with pytest.raises(ValueError, match=fname):
                get_user_defined_globals(fname)

        finally:
            os.remove(fname)
