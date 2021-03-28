import errno
import os
import random
from contextlib import contextmanager
from typing import Dict, Iterator

import luigi  # type: ignore


@contextmanager
def multi_target_manager(
    targets: Dict[str, luigi.target.FileSystemTarget],
) -> Iterator[Dict[str, str]]:
    """
    A context manager for working with multiple targets at once,
    to ensure that either all final outputs are written, or none.
    :param targets: A dict of the targets to write
    :return:

    :example:
    class MyTask(luigi.Task):
        def output(self):
            return {
                "a": LocalTarget(...),
                "b": LocalTarget(...),
            }
        def run(self):
            with multi_target_manager(self.output()) as tmp_files:
                run_some_external_command(output_path=tmp_files["a"])
                run_some_other_command(output_path=tmp_files["b"])
    """

    # Adapted from luigi.FileSystemTarget.temporary_path()
    # See https://github.com/spotify/luigi/blob/master/luigi/target.py

    # Generate temporary paths for all targets and ensure parent directories exist
    temp_paths = {name: _get_temp_path(target) for name, target in targets.items()}

    try:
        yield temp_paths

        # We want to either have all output files or none,
        # so check all exist
        missing_outputs = []

        for (name, target) in targets.items():
            if not os.path.exists(temp_paths[name]):
                missing_outputs.append(name)

        if any(missing_outputs):
            message = ", ".join(missing_outputs)
            paths = [temp_paths[name] for name in missing_outputs]

            raise FileNotFoundError(
                errno.ENOENT, f"Output(s) for {message} were not found", paths
            )

        # If all files were written to temp paths successfully,
        # then move to their final destinations.
        # Unfortunately, there is no way to do this completely atomically,
        # but hopefully this move is low risk in comparison to earlier steps.
        for (name, target) in targets.items():
            target.fs.move(temp_paths[name], target.path, raise_if_exists=False)
    except Exception as e:
        # Ensure any raised exceptions are propagated up
        raise
    finally:
        # Clean up temporary files if still exist
        # (e.g. if exception thrown during processing)
        for tmp_path in temp_paths.values():
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass


def _get_temp_path(target: luigi.target.FileSystemTarget) -> str:
    """
    Generate temporary path for a Luigi file system target,
    alongside the intended destination for the file.
    :param target: Luigi FileSystemTarget
    :return: Temporary path for target
    """
    # Make sure the parent directory of the target exists
    dir_path = os.path.dirname(target.path)
    target.fs.mkdir(dir_path, parents=True, raise_if_exists=False)

    # Construct a temporary filename with the same extension as the original
    filename = os.path.basename(target.path)
    num = random.randrange(0, 10000000000)
    temp_path = os.path.join(dir_path, "luigi-tmp-{:010}-{}".format(num, filename))

    return temp_path
