
import pyspark.sql.functions as F
_df_.agg(F.countDistinct("column_name"))
df.select("columnName").distinct().count()
