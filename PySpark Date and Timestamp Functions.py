
import pyspark
import pyspark.sql.functions as F

#loading the dataframes
transactions = spark.table('data_table_name.transactions')
partners = spark.table('data_table_name.partners')

#showing the tables 
display(transactions)
display(partners)

# checking all the column names and data types
transactions.printSchema()
partners.printSchema()

# creating transactions dataframe using relevant features
transactions_df = transactions.select("id","account_id","partner_id", "transaction_type","channel", "transaction_date","total_value","incentive_value","partner_code","currency_code")
transactions_df.limit(5).toPandas()

# creating partners dataframe using relevant features
partners_df = partners.select("id","code","category")
partners_df.limit(5).toPandas()

#joining transactions and partners table
transactions_partners = transactions_df.join(partners_df,transactions_df.partner_id == partners_df.id,"right")
transactions_partners.limit(5).toPandas()

transactions_partners.select(F.date_format('transaction_date','MM').alias('month')).groupby('month').count().show() 
transactions_partners.withColumn("todays_date", F.current_date()).show()
transactions_partners.withColumn("todays_date_time", F.current_timestamp()).show() 

transactions_partners = transactions_partners.withColumn("hour", F.hour(F.col("transaction_date")))
transactions_partners = transactions_partners.withColumn(
    'time_of_day',
    F.when((F.col("hour") >= 12) & (F.col("hour") < 16), "Afternoon")\
    .when((F.col("hour") >= 16) & (F.col('hour') < 20), "Evening")\
    .when((F.col("hour") >= 5) & (F.col('hour') < 12), "Morning")\
    .otherwise("Night")
)
transactions_partners.limit(10).toPandas()

transactions_partners = transactions_partners.withColumn("day", F.dayofweek(F.col("transaction_date")))
transactions_partners.limit(10).toPandas()

transactions_partners.groupby('day').count().orderBy('count', ascending=False).toPandas()




