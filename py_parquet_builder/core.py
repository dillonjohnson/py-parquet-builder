from uuid import uuid4
from schema import Schema, And, Use, Optional
from py_parquet_builder.s3 import upload_file
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import datetime
import os
from functools import partial

# facebook/facebook-page-insights/2020/04/file.parquet
class FailedToLoadException(Exception):
    pass


def validate_input(obj):
    schema = Schema([{'processed_timestamp': datetime.datetime,
                      'effective_timestamp': datetime.datetime,
                      'month': str,
                      'year': str,
                      'json_payload': str,
                      'social_network_id': str,
                      'data_type_data': str,
                      'social_network_type': str}],
                    ignore_extra_keys=True)
    schema.validate(obj)


def validate_output(obj):
    schema = Schema([{'processed_timestamp': datetime.datetime,
                      'effective_timestamp': datetime.datetime,
                      'month': str,
                      'year': str,
                      'json_payload': str,
                      'social_network_id': str,
                      'filename': str,
                      'data_type_data': str,
                      'social_network_type': str}],
                    ignore_extra_keys=True)
    schema.validate(obj)

def dictarray_to_parquet_table(obj):
    df = pd.DataFrame.from_dict(obj)
    table = pa.Table.from_pandas(df, preserve_index=False)
    return table


def parquet_table_to_file(tab,
                          filename):
    pq.write_table(table=tab,
                   where=filename)
    return filename


def update_filename(row, filename):
    row['filename'] = filename
    return row

def dictarray_file_to_s3(obj, bucket):
    # validate input
    validate_input(obj)

    # create and append filename
    # filename = uuid4() + ".parquet"
    filename = f'{obj["social_network_type"]}/{obj["data_type_data"]}/year={obj["year"]}/month={obj["month"]}/{uuid4()}.parquet'
    # obj['filename'] = filename
    obj = list(map(partial(update_filename, filename=filename), obj))

    # validate output
    validate_output(obj)

    # Convert to parquet table
    tab = dictarray_to_parquet_table(obj)

    # Load parquet to file
    filename = parquet_table_to_file(tab, filename)

    # Load file to S3
    resp = upload_file(filename, bucket)

    if resp:
        os.remove(filename)
    else:
        # Raise alerts
        os.remove(filename)
        raise FailedToLoadException(f"The file could not be loaded to bucket. Please retry again later.")



if __name__ == '__main__':
    obj = [
        {
            "processed_timestamp": datetime.datetime.now(),
            "effective_timestamp": datetime.datetime.today(),
            "year": "2020",
            "month": "06",
            "test": "test",
            "json_payload": "",
            "social_network_id": ""
        }
    ]
    # validate_input(obj)
    # dictarray_to_bytes(obj)
    dictarray_file_to_s3(obj)
