# Luigi Utilities

[![Tests](https://github.com/mikelynch/luigi-utilities/actions/workflows/python-tests.yml/badge.svg)](https://github.com/mikelynch/luigi-utilities/actions/workflows/python-tests.yml)

This package provides additional functionality to make Luigi more flexible.

## `new_task` and `pick_output`

Out of the box, Luigi requires you to explicitly list
the dependencies of a task in the task itself, e.g.:

```python
import luigi

class SquareTask(luigi.Task):
    def requires(self):
        return SomeOtherTask()
    
    def output(self):
        return luigi.LocalTarget("squared.txt")

    def run(self):
        with self.input().open("r") as input_:
            with self.output().open("w") as f:
                for line in input_:
                    num = int(line)
                    f.write(f"{num * num}\n")
```

This can be fine for simple workflows,
but sometimes it is useful to be able to use tasks more generally.
(For example in this case, allowing the `SquareTask` to run on other inputs.)

The `new_task` function adds this functionality, allowing you to do:

```python
import luigi

from luigi_utilities import new_task

class SquareTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget("squared.txt")

    def run(self):
        with self.input().open("r") as input_:
            with self.output().open("w") as f:
                for line in input_:
                    num = int(line)
                    f.write(f"{num * num}\n")

other = SomeOtherTask()
square = new_task(SquareTask, other)
```

Now `SomeOtherTask` can be swapped out for other tasks
that produce similar output.
This makes Luigi tasks into reusable blocks
that can be reused in complex pipelines.

`new_task` can take individual tasks, lists of tasks, or dictionaries of tasks,
similarly to how `requires` works normally.

To facilitate consuming tasks which produce multiple outputs,
there is another function called `pick_output`,
which selects one output to pass on to another task.
This can be used like this:

```python
import luigi

from luigi_utilities import new_task, pick_task

class OddEvenTask(luigi.Task):
    def output(self):
        return {
            "odd": luigi.LocalTarget("odd.txt"),
            "even": luigi.LocalTarget("even.txt"),
        }

odd_even = OddEvenTask()

# OtherTask only expects a single input
other_task = new_task(OtherTask, pick_output(odd_even, "even"))
```

For more usage examples, see the unit tests.

## `multi_target_manager`

Sometimes it is necessary to have multiple outputs from a task.
If this is done naively, just using `luigi.LocalTarget`,
then while each file will be written atomically
(i.e. it will only exist in the output if it was successfully written),
it is possible to have one output written, but not others.
This can lead to workflow failures and confusion.

This generalizes the idea in the built-in context manager in `luigi.Target`
to support writing multiple temporary output files.
Only if these are all written successfully
are they renamed to their target filenames, e.g.:

```python
import luigi

from luigi_utilities import multi_target_manager

class MyTask(luigi.Task):
    def output(self):
        return {
            "a": luigi.LocalTarget("a.txt"),
            "b": luigi.LocalTarget("b.txt"),
        }

    def run(self):
        with multi_target_manager(self.output()) as tmp_files:
            run_some_external_command(output_path=tmp_files["a"])
            run_some_other_command(output_path=tmp_files["b"])
```

This will ensure that either both files were written to successfully,
in which case `a.txt` and `b.txt` will both exist,
or that neither do.

## `AutoRerunTask`

By default, Luigi tasks are only run if any of their outputs are missing.
Imagine a pipeline that takes an input file,
and then runs tasks which transform that data,
which has been run to completion once.
If the input file was changed,
and this pipeline was run again,
the output from the pipeline would not be recalculated,
as the output of the final task would already exist.

`AutoRerunTask` is a subtype of `luigi.Task`,
which adds functionality to generate hashes of
the input data and parameters,
so that if these are changed,
the tasks are considered incomplete,
and will be rerun.
This is done by calculating a hash which represents the output of the task,
which is written to disk.
On subsequent runs of the pipeline,
this hash value is compared with the current value.
The hash of a task depends on
the hashes of its inputs,
and of all its parameters, e.g.:

```python
import luigi

from luigi_utilities import AutoRerunTask

class SequentialNumbersTask(AutoRerunTask):
    min_number = luigi.IntParameter()
    max_number = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget("seq.txt")

    def run(self):
        with self.output().open("w") as f:
            for i in range(self.min_number, self.max_number):
                f.write(f"{i}\n")
```

When this task is run, the file `seq.txt` is created.
If the values of `min_number` or `max_number` are changed,
then the value of the hash for this task would change,
so that the task would be rerun.

An `AutoRerunTask` is only considered complete if:
* All of its outputs exist
* Its hash exists on disk
* The hash on disk matches the current hash value
* All of the task's requirements (i.e. ancestors) are complete

Currently, the value of a task's hash depends on:
* The hashes of all its requirements
* All the parameters for the task

This means that `AutoRerunTask` and regular `luigi.Task` cannot currently be mixed,
as the `AutoRerunTask` needs to be able to check the hash of earlier tasks
in the pipeline.
