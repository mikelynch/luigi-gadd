import os
import pickle
from datetime import datetime
from tempfile import TemporaryDirectory

import luigi

from luigi_utilities import AutoRerunTask, RerunExtTask, new_task


def test_auto_rerun_task_for_input_changes():
    # Test that AutoRerunTasks correctly detect changes
    # to input earlier in their pipeline
    with TemporaryDirectory() as tmp_dir:
        # Set up directory to write hashes to
        hash_dir = os.path.join(tmp_dir, "hashes")
        os.mkdir(hash_dir)

        input_path = os.path.join(tmp_dir, "input.txt")

        # Set up pipeline
        input_task = RerunExtTask(hash_dir, input_path)
        word_count = new_task(
            WordCountPerLine,
            input_task,
            {
                "hash_dir": hash_dir,
                "directory": tmp_dir,
            },
        )
        sum_ = new_task(
            SumValues,
            word_count,
            {
                "hash_dir": hash_dir,
                "directory": tmp_dir,
            },
        )

        # Set up input file with initial data
        _write_example_file(input_path, 4)

        # Run pipeline and verify results
        result = luigi.build([sum_], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(sum_.output().path, "rt") as f:
            total = int(f.read())
        assert total == 28

        # Get when tasks last run
        with open(os.path.join(tmp_dir, "WordCount.pickle"), "rb") as f:
            first_word_count_ran = pickle.load(f)
        with open(os.path.join(tmp_dir, "SumValues.pickle"), "rb") as f:
            first_sum_values_ran = pickle.load(f)

        assert first_word_count_ran < first_sum_values_ran

        # Rerun pipeline with no changes and verify results
        result = luigi.build([sum_], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(sum_.output().path, "rt") as f:
            total = int(f.read())
        assert total == 28

        # Get when tasks last run
        with open(os.path.join(tmp_dir, "WordCount.pickle"), "rb") as f:
            second_word_count_ran = pickle.load(f)
        with open(os.path.join(tmp_dir, "SumValues.pickle"), "rb") as f:
            second_sum_values_ran = pickle.load(f)

        # Ensure tasks have not been run again (as no changes)
        assert first_word_count_ran == second_word_count_ran
        assert first_sum_values_ran == second_sum_values_ran

        # Change input file which should cause rerun of entire pipeline
        _write_example_file(input_path, 8)

        # Rerun pipeline with changes and verify results
        result = luigi.build([sum_], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(sum_.output().path, "rt") as f:
            total = int(f.read())
        assert total == 57

        # Get when tasks last run
        with open(os.path.join(tmp_dir, "WordCount.pickle"), "rb") as f:
            third_word_count_ran = pickle.load(f)
        with open(os.path.join(tmp_dir, "SumValues.pickle"), "rb") as f:
            third_sum_values_ran = pickle.load(f)

        # Ensure tasks have been run again
        assert third_word_count_ran < third_sum_values_ran
        assert first_word_count_ran < third_word_count_ran
        assert first_sum_values_ran < third_sum_values_ran


def test_auto_rerun_task_for_parameter_changes():
    # Test that AutoRerunTasks correctly detect changes
    # to parameters earlier in their pipeline
    with TemporaryDirectory() as tmp_dir:
        # Set up directory to write hashes to
        hash_dir = os.path.join(tmp_dir, "hashes")
        os.mkdir(hash_dir)

        # Set up pipeline
        seq_ = SequentialNumbersTask(hash_dir, tmp_dir, 0, 5)
        sum_ = new_task(
            SumValues,
            seq_,
            {
                "hash_dir": hash_dir,
                "directory": tmp_dir,
            },
        )

        # Run pipeline and verify results
        result = luigi.build([sum_], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(sum_.output().path, "rt") as f:
            total = int(f.read())
        assert total == 10

        # Get when tasks last run
        with open(os.path.join(tmp_dir, "SumValues.pickle"), "rb") as f:
            first_sum_values_ran = pickle.load(f)

        # Rerun pipeline with no changes and verify results
        result = luigi.build([sum_], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(sum_.output().path, "rt") as f:
            total = int(f.read())
        assert total == 10

        # Get when tasks last run
        with open(os.path.join(tmp_dir, "SumValues.pickle"), "rb") as f:
            second_sum_values_ran = pickle.load(f)

        # Ensure tasks have not been run again (as no changes)
        assert first_sum_values_ran == second_sum_values_ran

        # Change input parameters which should cause rerun of entire pipeline
        seq_ = SequentialNumbersTask(hash_dir, tmp_dir, 0, 10)
        sum_ = new_task(
            SumValues,
            seq_,
            {
                "hash_dir": hash_dir,
                "directory": tmp_dir,
            },
        )

        # Rerun pipeline with changes and verify results
        result = luigi.build([sum_], detailed_summary=True, local_scheduler=True)
        assert result.status == luigi.LuigiStatusCode.SUCCESS

        with open(sum_.output().path, "rt") as f:
            total = int(f.read())
        assert total == 45

        # Get when tasks last run
        with open(os.path.join(tmp_dir, "SumValues.pickle"), "rb") as f:
            third_sum_values_ran = pickle.load(f)

        # Ensure tasks have been run again
        assert first_sum_values_ran < third_sum_values_ran


def _write_example_file(path: str, num_lines: int):
    LINES = [
        "Two households, both alike in dignity,",
        "In fair Verona, where we lay our scene,",
        "From ancient grudge break to new mutiny,",
        "Where civil blood makes civil hands unclean.",
        "From forth the fatal loins of these two foes",
        "A pair of star-cross'd lovers take their life;",
        "Whose misadventured piteous overthrows",
        "Do with their death bury their parents' strife.",
    ]

    with open(path, "wt") as f:
        for line, _i in zip(LINES, range(num_lines)):
            f.write(f"{line}\n")


class WordCountPerLine(AutoRerunTask):
    directory = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, "word_counts.txt"))

    def run(self):
        with self.input().open("r") as input_:
            with self.output().open("w") as output_:
                for line in input_:
                    pieces = line.strip().split()
                    output_.write(f"{len(pieces)}\n")

        # Also write when ran to disk
        with open(os.path.join(self.directory, "WordCount.pickle"), "wb") as f:
            pickle.dump(datetime.now(), f)


class SumValues(AutoRerunTask):
    directory = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, "total.txt"))

    def run(self):
        total = 0

        with self.input().open("r") as input_:
            for line in input_:
                try:
                    value = int(line)
                except:
                    value = 0

                total += value

        with self.output().open("w") as output_:
            output_.write(f"{total}\n")

        # Also write when run to disk
        with open(os.path.join(self.directory, "SumValues.pickle"), "wb") as f:
            pickle.dump(datetime.now(), f)


class SequentialNumbersTask(AutoRerunTask):
    directory = luigi.Parameter()
    min_number = luigi.IntParameter()
    max_number = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.directory, f"seq.txt"))

    def run(self):
        with self.output().open("w") as f:
            for i in range(self.min_number, self.max_number):
                f.write(f"{i}\n")
