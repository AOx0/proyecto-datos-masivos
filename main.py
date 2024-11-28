# %%
import base64
import polars as pl
import urllib.request
import functions_framework

from google.cloud import bigquery
from cloudevents.http import CloudEvent


# %%
def create_table(df: pl.DataFrame, column: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    # Create new dataframe with ID and column values
    new_df = (
        df.lazy()
        .select(
            pl.col(column).unique().rank("min").alias("id").cast(pl.UInt16) - 1,
            pl.col(column).unique(),
        )
        .sort("id")
        .collect()
    )

    # Replace values in original df with IDs
    value_map = dict(zip(new_df[column], new_df["id"]))
    df = (
        df.lazy()
        .with_columns(pl.col(column).replace_strict(value_map, return_dtype=pl.UInt16))
        .collect()
    )

    return (df, new_df)


# %%
def print_columns(df):
    for column in df.get_columns():
        uniques = column.n_unique()
        print(f"Uniques for col {column.name}: {uniques}", end="")
        if uniques < 250:
            uniques = column.unique().to_list()
            print(f": {uniques[:10]}")
        else:
            print("")


def df_to_bigquery(df: pl.DataFrame, project_id: str, dataset_id: str, table_id: str, clustered_by: list[str] | None = None):
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Delete table if it exists
    try:
        client.delete_table(table_ref)
        print(f"Table {table_ref} deleted.")
    except:
        print(f"Table {table_ref} did not exist.")

    # Convert Polars schema to BigQuery schema
    schema = []
    for col in df.schema.items():
        name = col[0]
        dtype = str(col[1])
        if "Int" in dtype or "UInt" in dtype:
            bq_type = "INTEGER"
        elif dtype == "Float32" or dtype == "Float64":
            bq_type = "FLOAT"
        elif dtype == "Boolean":
            bq_type = "BOOLEAN"
        elif dtype == "Date":
            bq_type = "DATE"
        elif dtype == "Time":
            bq_type = "TIME"
        else:
            bq_type = "STRING"

        # Check if column has null values
        is_nullable = df[name].null_count() > 0

        # Make id columns non-nullable primary keys
        schema.append(bigquery.SchemaField(name, bq_type, mode='NULLABLE' if is_nullable else 'REQUIRED'))

    # Create table with clustering on specified columns
    table = bigquery.Table(table_ref, schema=schema)
    if clustered_by is not None:
        table.clustering_fields = clustered_by
    elif "id" in df.columns:
        table.clustering_fields = ["id"]

    table = client.create_table(table)

    # Convert to pandas and upload (BigQuery client doesn't support Polars directly)
    job_config = bigquery.LoadJobConfig(schema=schema)
    client.load_table_from_dataframe(df.to_pandas(), table_ref, job_config=job_config)

# Create traffic_data dataset if it doesn't exist
def create_dataset_if_not_exists(client: bigquery.Client, dataset_name: str):
    dataset_id = f"{client.project}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_id}")

# %%
@functions_framework.cloud_event
def my_cloudevent_function(
    cloud_event: CloudEvent,
):
    URL = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    print("Saving " + URL)

    cache_filename = "downloaded_data.csv"

    try:
        # Try to open cached file first
        response = open(cache_filename, "rb")
        print("Using cached file")
    except FileNotFoundError:
        # If cache doesn't exist, download and save
        print("Downloading fresh copy")
        req = urllib.request.Request(url=URL)
        response = urllib.request.urlopen(req)
        with open(cache_filename, "wb") as f:
            f.write(response.read())
        response = open(cache_filename, "rb")

    DIAS = { "Lunes": 0, "Martes": 1, "Miércoles": 2, "Miercoles": 2, \
                     "Jueves": 3, "Viernes": 4, "Sabado": 5, "Sábado": 5, "Domingo": 6 }  # fmt: skip
    SINO = { "SI": True, "NO": False }
    PRIORIDAD = { "ALTA": 2, "MEDIA": 1, "BAJA": 0 }

    df = (
        pl
        .read_csv(response, infer_schema_length=2 ** (64 - 1), null_values=["NA"])
        .lazy()
        .filter(pl.col("dia").is_in(DIAS.keys()))
        .with_columns(
            pl.col("prioridad").replace_strict(PRIORIDAD).cast(pl.UInt8),
            pl.col("dia").replace_strict(DIAS).cast(pl.UInt8),
            pl.col("fecha_evento").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
            pl.col("hora_evento").str.strptime(pl.Time, format="%H:%M:%S", strict=False),
            pl.col("fecha_captura").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
            pl.col("trasladado_lesionados").replace_strict(SINO, default=False, return_dtype=pl.Boolean),
            pl.col("interseccion_semaforizada").replace_strict(SINO, default=False, return_dtype=pl.Boolean),


        )
    ).collect()

    for column in df.get_columns():
        name = column.name
        ws = ['_de_', '_la_', '_a_']
        while sum((1 if w in name else 0 for w in ws)) != 0:
            for w in ws:
                name = name.replace(w, '_')
        df = df.rename({column.name: name})

    table_names = [
        col.name
        for col in df.get_columns()
        if col.n_unique() < 50 and col.name not in ["dia"] and col.dtype == pl.Utf8
    ] # fmt: skip
    print(f">> Processing {len(table_names)} columns: {table_names}")
    print_columns(df[table_names])
    print("\n")

    tables = {}
    for name in table_names:
        df, tables[name] = create_table(df, name)

    tables["dia"] = (pl.DataFrame(
        {
            "id": [0, 1, 2, 3, 4, 5, 6],
            "dia": ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"],  # fmt: skip
        }
    ).with_columns(pl.col("id").cast(pl.UInt8)))

    tables["prioridad"] = (pl.DataFrame(
        {
            "id": [0, 1, 2],
            "prioridad": ["BAJA", "MEDIA", "ALTA"],
        }
    ).with_columns(pl.col("id").cast(pl.UInt8)))

    print(">> Result:")
    print_columns(df[table_names])
    print("\n")

    client = bigquery.Client()
    create_dataset_if_not_exists(client, "traffic_data")

    # Upload tables
    df_to_bigquery(df, client.project, "traffic_data", "events", clustered_by=['fecha_evento', 'alcaldia', 'tipo_evento', 'sector'])
    for name, df in tables.items():
        df_to_bigquery(df, client.project, "traffic_data", name)

    print("Done!")
