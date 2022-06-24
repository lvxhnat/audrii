"""
This is a boilerplate pipeline 'ingestion'
generated using Kedro 0.18.1
"""

from kedro.pipeline import Pipeline, node, pipeline

from audrii.pipelines.ingestion.nodes import get_russell_3000_tickers, ingest_russell_3000

ingestion_parameters = node(
    func=get_russell_3000_tickers,
    inputs="raw_russell_indexes",
    outputs="tickers",
    name="get_tickers"
)
ingest_data = node(
    func=ingest_russell_3000,
    inputs=["tickers", "params:ingestion_parameters"],
    outputs="raw_ticker_data",
    name="extract_ingestion_data"
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([ingestion_parameters, ingest_data])
