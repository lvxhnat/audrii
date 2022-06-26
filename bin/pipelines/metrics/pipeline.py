"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline
from audrii.pipelines.metrics.nodes import scrape_or_ingest_data, merge_ingested_data

ingest_data = node(
    func=scrape_or_ingest_data,
    inputs="params:data_ingestion",
    outputs="raw_ticker_data",
    name="load_data"
)

merge_ingested_data = node(
    func=merge_ingested_data,
    inputs=["params:data_ingestion", "raw_ticker_data"],
    outputs="merged_raw_ticker_data",
    name="merge_data"
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [ingest_data, merge_ingested_data]
    )
