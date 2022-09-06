_df_.groupBy('column_name').count().orderBy('count', ascending=False).toPandas()
