import os
import pickle
from datetime import datetime
from tempfile import TemporaryDirectory

import luigi

from luigi_utilities import (
    AutoRerunTask,
    RerunExtTask,
    multi_target_manager,
    new_task,
    pick_output,
)


def test_auto_rerun_task_for_input_changes():
    # Test that AutoRerunTasks correctly detect changes
    # to input earlier in their pipeline
    with TemporaryDirectory() as tmp_dir:
        # Set up directory to write hashes to
        hash_dir = os.path.join(tmp_dir, "hashes")
        os.mkdir(hash_dir)

        seq = SequentialNumbersTask(hash_dir, tmp_dir, 0, 10)
        filter_ = new_task(
            FilterEvenOddTask,
            seq,
            {"hash_dir": hash_dir, "directory": tmp_dir},
        )
        filter0_ = new_task(
            FilterEvenOddTask,
            pick_output(filter_, "even"),
            {"hash_dir": hash_dir, "directory": tmp_dir, "suffix": "0"},
        )
        filter1_ = new_task(
            FilterEvenOddTask,
            pick_output(filter_, "odd"),
            {"hash_dir": hash_dir, "directory": tmp_dir, "suffix": "1"},
        )

        result = luigi.build(
            [filter0_, filter1_], detailed_summary=True, local_scheduler=True
        )
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(filter0_.output()["even"].path, "rt") as f:
            even = [int(l) for l in f.readlines()]

        assert len(even) == 5
        assert even[0] == 0
        assert even[1] == 2

        with open(filter1_.output()["odd"].path, "rt") as f:
            odd = [int(l) for l in f.readlines()]

        assert len(odd) == 5
        assert odd[0] == 1
        assert odd[1] == 3

        # Change pipeline
        seq = SequentialNumbersTask(hash_dir, tmp_dir, 10, 30)
        filter_ = new_task(
            FilterEvenOddTask,
            seq,
            {"hash_dir": hash_dir, "directory": tmp_dir},
        )
        filter0_ = new_task(
            FilterEvenOddTask,
            pick_output(filter_, "even"),
            {"hash_dir": hash_dir, "directory": tmp_dir, "suffix": "0"},
        )
        filter1_ = new_task(
            FilterEvenOddTask,
            pick_output(filter_, "odd"),
            {"hash_dir": hash_dir, "directory": tmp_dir, "suffix": "1"},
        )

        # Rerun pipeline with changes and verify results
        result = luigi.build(
            [filter0_, filter1_], detailed_summary=True, local_scheduler=True
        )
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(filter0_.output()["even"].path, "rt") as f:
            even = [int(l) for l in f.readlines()]

        assert len(even) == 10
        assert even[0] == 10
        assert even[1] == 12

        with open(filter1_.output()["odd"].path, "rt") as f:
            odd = [int(l) for l in f.readlines()]

        assert len(odd) == 10
        assert odd[0] == 11
        assert odd[1] == 13


class SequentialNumbersTask(AutoRerunTask):
    directory = luigi.Parameter()
    min_number = luigi.IntParameter()
    max_number = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.directory, f"seq-{self.min_number}-{self.max_number}.txt")
        )

    def run(self):
        with self.output().open("w") as f:
            for i in range(self.min_number, self.max_number):
                f.write(f"{i}\n")


class FilterEvenOddTask(AutoRerunTask):
    directory = luigi.Parameter()
    suffix = luigi.Parameter(default="")

    def output(self):
        return {
            "odd": luigi.LocalTarget(
                os.path.join(self.directory, f"odd{self.suffix}.txt")
            ),
            "even": luigi.LocalTarget(
                os.path.join(self.directory, f"even{self.suffix}.txt")
            ),
        }

    def run(self):
        with self.input().open("r") as input_:
            with multi_target_manager(self.output()) as tmp_files:
                with open(tmp_files["odd"], "w", encoding="utf-8") as odd:
                    with open(tmp_files["even"], "w", encoding="utf-8") as even:
                        for line in input_:
                            num = int(line)

                            if num % 2 == 0:
                                even.write(f"{num}\n")
                            else:
                                odd.write(f"{num}\n")


class AddNumbersTask(AutoRerunTask):
    directory = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, "sum.txt"))

    def run(self):
        with self.input()["lhs"].open("r") as lhs:
            with self.input()["rhs"].open("r") as rhs:
                with self.output().open("w") as tmp_file:
                    for l, r in zip(lhs, rhs):
                        add = int(l) + int(r)
                        tmp_file.write(f"{add}\n")
