from typing import Dict, List, TypeVar, Union

import luigi  # type: ignore

K = TypeVar("K")
T = TypeVar("T", bound=luigi.Task)


def new_task(
    task_class: T,
    params={},
    requires=Union[luigi.Task, List[luigi.Task], Dict[K, luigi.Task]],
    task_name=None,
) -> T:
    """Instantiate task, dynamically specifying its requirements.

    :param task_class: Task type to instantiate
    :param params: Parameters to pass to the new task
    :param requires: Requirements for task
    :param task_name: Optional name to use for new task
    :return: New task instance, with dependencies dynamically set
    """
    # Set default task name
    new_task_name = task_class.__name__
    if task_name is not None:
        new_task_name = task_name

    # Dynamically create sub-type of the task class,
    # where we specify the requirements
    ctor = type(
        new_task_name,
        (task_class, object),
        {
            "requires": lambda self: requires,
        },
    )

    return ctor(**params)


class ExtractOutputTask(luigi.Task):
    """Helper task type used for unpacking multiple outputs"""

    output_name = luigi.Parameter()

    def output(self):
        return self.input()[self.output_name]


def pick_output(task: luigi.Task, output_name: str) -> ExtractOutputTask:
    """Pick a single output from a dict with multiple ones

    :param task: Task with multiple outputs
    :param output_name: Output to select
    :return: New task instance, with dependencies dynamically set
    """
    return new_task(ExtractOutputTask, {"output_name": output_name}, task)