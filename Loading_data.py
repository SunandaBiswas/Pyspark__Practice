#loading the data tables in databricks platform

transactions = spark.table('data_table.transactions')
partners = spark.table('data_table.partners')


#showing the tables 

display(transactions)
display(partners)


# checking all the column names and data types

transactions.printSchema()
partners.printSchema()


# creating transactions dataframe using relevant features

transactions_df = transactions.select("id","account_id","partner_id", "transaction_type","channel", "transaction_date","total_value","incentive_value","partner_code","currency_code")
transactions_df.limit(5).toPandas()
