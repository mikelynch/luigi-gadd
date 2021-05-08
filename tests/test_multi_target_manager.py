import os
from tempfile import TemporaryDirectory
from typing import Dict

import luigi
import pytest

from luigi_gadd import multi_target_manager


def _get_output_dict(dir_name: str) -> Dict[str, luigi.LocalTarget]:
    # Final paths to write data to
    first_path = os.path.join(dir_name, "first.txt")
    second_path = os.path.join(dir_name, "second.txt")

    return {
        "first": luigi.LocalTarget(first_path),
        "second": luigi.LocalTarget(second_path),
    }


def test_outputs_all_files():
    with TemporaryDirectory() as tmp_dir:
        outputs = _get_output_dict(tmp_dir)

        with multi_target_manager(outputs) as tmp_files:
            with open(tmp_files["first"], "w") as f:
                f.write("first")

            with open(tmp_files["second"], "w") as f:
                f.write("second")

        # Test that output is written to final paths
        with open(outputs["first"].path, "rt") as f:
            first = f.read()

        with open(outputs["second"].path, "rt") as f:
            second = f.read()

        assert first == "first"
        assert second == "second"


def test_outputs_no_files_if_incomplete():
    with TemporaryDirectory() as tmp_dir:
        outputs = _get_output_dict(tmp_dir)

        with pytest.raises(FileNotFoundError) as exc_info:
            with multi_target_manager(outputs) as tmp_files:
                with open(tmp_files["first"], "w") as f:
                    f.write("first")

        # Test that no output
        assert not os.path.exists(outputs["first"].path)
        assert not os.path.exists(outputs["second"].path)

        assert "second" in str(exc_info.value)


def test_outputs_none_on_exception():
    with TemporaryDirectory() as tmp_dir:
        outputs = _get_output_dict(tmp_dir)

        with pytest.raises(ZeroDivisionError) as exc_info:
            with multi_target_manager(outputs) as tmp_files:
                with open(tmp_files["first"], "w") as f:
                    f.write("first")

                with open(tmp_files["second"], "w") as f:
                    f.write("second")

                # Deliberately cause exception
                a = 1 / 0

        # Test that no output
        assert not os.path.exists(outputs["first"].path)
        assert not os.path.exists(outputs["second"].path)


def test_temp_files_cleaned_up_on_failure():
    with TemporaryDirectory() as tmp_dir:
        outputs = _get_output_dict(tmp_dir)

        paths = {}

        with pytest.raises(ZeroDivisionError) as exc_info:
            with multi_target_manager(outputs) as tmp_files:
                paths = tmp_files.copy()

                with open(tmp_files["first"], "w") as f:
                    f.write("first")

                with open(tmp_files["second"], "w") as f:
                    f.write("second")

                # Deliberately cause exception
                a = 1 / 0

        # Test that no temp files left
        assert not os.path.exists(paths["first"])
        assert not os.path.exists(paths["second"])
