import os
from tempfile import TemporaryDirectory

import luigi

from luigi_utilities import multi_target_manager, new_task, pick_output


def test_single_output_to_single_input():
    # Test graph (right nodes depend on nodes to their left):
    # Seq ─ Filter
    with TemporaryDirectory() as tmp_dir:
        seq = SequentialNumbersTask(tmp_dir, 0, 10)
        filter_ = new_task(
            FilterEvenOddTask,
            seq,
            {"directory": tmp_dir},
        )

        result = luigi.build([filter_], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(filter_.output()["even"].path, "rt") as f:
            even = [int(l) for l in f.readlines()]

        with open(filter_.output()["odd"].path, "rt") as f:
            odd = [int(l) for l in f.readlines()]

        assert even[-1] == 8
        assert odd[-1] == 9


def test_multiple_single_outputs_to_multiple_inputs():
    # Test graph (right nodes depend on nodes to their left):
    # Seq0
    #     ╲
    #       ─ Add
    #     ╱
    # Seq1
    with TemporaryDirectory() as tmp_dir:
        seq0 = SequentialNumbersTask(tmp_dir, 0, 10)
        seq1 = SequentialNumbersTask(tmp_dir, 100, 110)
        add = new_task(
            AddNumbersTask,
            {
                "lhs": seq0,
                "rhs": seq1,
            },
            {"directory": tmp_dir},
        )

        result = luigi.build([add], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(add.output().path, "rt") as f:
            sums = [int(l) for l in f.readlines()]

        assert sums[0] == 100
        assert sums[1] == 102


def test_multiple_outputs_to_multiple_inputs_on_single_task():
    # Test graph (right nodes depend on nodes to their left):
    # Seq ─ Filter ═ Add (i.e. Add consumes both outputs of Filter)
    with TemporaryDirectory() as tmp_dir:
        seq = SequentialNumbersTask(tmp_dir, 0, 10)
        filter_ = new_task(
            FilterEvenOddTask,
            seq,
            {"directory": tmp_dir},
        )
        add = new_task(
            AddNumbersTask,
            {
                "lhs": pick_output(filter_, "even"),
                "rhs": pick_output(filter_, "odd"),
            },
            {"directory": tmp_dir},
        )

        result = luigi.build([add], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(add.output().path, "rt") as f:
            sums = [int(l) for l in f.readlines()]

        assert sums[0] == 1
        assert sums[1] == 5


def test_multiple_outputs_to_separate_tasks():
    # Test graph (right nodes depend on nodes to their left):
    #                Filter0
    #              ╱
    # Seq ─ Filter
    #              ╲
    #                Filter1
    with TemporaryDirectory() as tmp_dir:
        seq = SequentialNumbersTask(tmp_dir, 0, 10)
        filter_ = new_task(
            FilterEvenOddTask,
            seq,
            {"directory": tmp_dir},
        )
        filter0_ = new_task(
            FilterEvenOddTask,
            pick_output(filter_, "even"),
            {"directory": tmp_dir, "suffix": "0"},
        )
        filter1_ = new_task(
            FilterEvenOddTask,
            pick_output(filter_, "odd"),
            {"directory": tmp_dir, "suffix": "1"},
        )

        result = luigi.build(
            [filter0_, filter1_], detailed_summary=True, local_scheduler=True
        )
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(filter0_.output()["even"].path, "rt") as f:
            even = [int(l) for l in f.readlines()]

        assert even[0] == 0
        assert even[1] == 2

        with open(filter1_.output()["odd"].path, "rt") as f:
            odd = [int(l) for l in f.readlines()]

        assert odd[0] == 1
        assert odd[1] == 3


def test_multiple_outputs_to_multiple_inputs_on_single_task2():
    # Test graph (right nodes depend on nodes to their left):
    #                Filter0
    #              ╱
    # Seq ─ Filter
    #              ╲
    #                Filter1
    with TemporaryDirectory() as tmp_dir:
        seq0 = SequentialNumbersTask(tmp_dir, 0, 10)
        filter_ = new_task(
            FilterEvenOddTask,
            seq0,
            {"directory": tmp_dir},
        )
        seq1 = SequentialNumbersTask(tmp_dir, 100, 110)
        add = new_task(
            AddNumbersTask,
            {
                "lhs": pick_output(filter_, "even"),
                "rhs": seq1,
            },
            {"directory": tmp_dir},
        )

        result = luigi.build([add], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(add.output().path, "rt") as f:
            sums = [int(l) for l in f.readlines()]

        assert sums[0] == 100
        assert sums[1] == 103


class SequentialNumbersTask(luigi.Task):
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


class FilterEvenOddTask(luigi.Task):
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


class AddNumbersTask(luigi.Task):
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
