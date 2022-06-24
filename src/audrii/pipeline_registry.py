"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline
from audrii.pipelines import ingestion


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.
    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    _pipeline = ingestion.create_pipeline()

    return {
        "__default__": _pipeline,
        "dp": _pipeline,
    }
