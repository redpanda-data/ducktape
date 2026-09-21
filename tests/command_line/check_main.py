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

from ducktape.command_line.main import get_user_defined_globals
from ducktape.command_line.main import print_test_symbols
from ducktape.command_line.main import setup_results_directory
from ducktape.command_line.main import update_latest_symlink

import io
import json
import os
import os.path
import pickle
import pytest
import tempfile

from mock import Mock


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


def collected(test_id, injected_args):
    return Mock(file='f.py', cls_name='C', function_name='m',
                test_id=test_id, injected_args=injected_args)


class CheckPrintTestSymbols(object):
    def print(self, tests):
        stdout, stderr = io.StringIO(), io.StringIO()
        status = print_test_symbols(tests, stdout, stderr)
        return status, stdout.getvalue(), stderr.getvalue()

    def check_one_symbol_per_test(self):
        status, stdout, stderr = self.print([collected('a', {'x': 1}), collected('b', None)])
        assert status == 0
        assert stdout == 'f.py::C.m@{"x":1}\nf.py::C.m\n'
        assert stderr == ''

    def check_fails_without_listing_anything(self):
        """A short list would look complete, so an unaddressable test fails the run."""
        status, stdout, stderr = self.print([collected('a', {'x': 1}), collected('b', {'x': (1, 2)})])
        assert status == 1
        assert stdout == ''
        assert 'No symbol for b' in stderr
        assert 'Cannot address 1 of 2' in stderr
