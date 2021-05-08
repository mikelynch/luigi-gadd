import pkg_resources

from luigi_gadd.auto_rerun_task import AutoRerunTask, RerunExtTask
from luigi_gadd.dynamic import new_task, pick_output
from luigi_gadd.multi_target_manager import multi_target_manager

__version__ = pkg_resources.get_distribution("luigi-gadd").version


__all__ = [
    "__version__",
    "AutoRerunTask",
    "RerunExtTask",
    "multi_target_manager",
    "new_task",
    "pick_output",
]
