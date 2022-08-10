#loading the data tables in databricks platform

transactions = spark.table('data_table.transactions')
partners = spark.table('data_table.partners')


#showing the tables 

display(transactions)
display(partners)
