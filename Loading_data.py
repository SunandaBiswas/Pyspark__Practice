#loading the data tables in databricks platform

transactions = spark.table('bronze_clm.transactions')
partners = spark.table('bronze_clm.partners')


#showing the tables 

display(transactions)
display(partners)
