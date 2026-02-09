from pyspark.sql.functions import expr


def delay_type_transform(df):
  delay_expr = expr(
    """case when WeatherDelay != 'NA'
              then 'WeatherDelay'
            when NASDelay != 'NA'
              then 'NASDelay'
            when SecurityDelay != 'NA'
              then 'SecurityDelay'
            when LateAircraftDelay != 'NA'
              then 'LateAircraftDelay'
            when IsArrDelayed == 'YES' OR IsDepDelayed == 'YES'
              then 'UncategorizedDelay'
        end
  """)
  return df.withColumn("delay_type", delay_expr)