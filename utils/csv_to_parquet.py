from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).parent.parent / "data"

def csv_to_parquet_large(csv_path, parquet_path, chunksize=100_000):
    writer = None

    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if writer is None:
            writer = pq.ParquetWriter(
                parquet_path,
                table.schema,
                compression="snappy"
            )

        writer.write_table(table)

    if writer:
        writer.close()

csv_to_parquet_large(
    BASE_DIR / "delivery" / "delivery_brz.csv",
    BASE_DIR / "delivery" / "delivery.parquet"
)

df = pd.read_parquet(BASE_DIR / "delivery" / "delivery.parquet", engine="pyarrow")
print(df.head())