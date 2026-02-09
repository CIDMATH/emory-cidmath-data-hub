from pyspark.sql.functions import current_timestamp, current_date, col

def add_last_updated_datetime_column(df):
    df = df.withColumn("last_updated_datetime", current_timestamp())
    return df

def add_last_updated_datetime_column(df):
    df = df.withColumn("last_updated_datetime", current_timestamp())
    return df