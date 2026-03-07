import sys
import pandas as pd
from google.cloud import bigquery


def query_to_markdown(query: str) -> str:
    client = bigquery.Client()
    df = client.query(query).to_dataframe()
    return df.to_markdown(index=False)


if __name__ == "__main__":
    query = sys.argv[1]
    print(query_to_markdown(query))
