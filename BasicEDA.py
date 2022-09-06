## Loading data with pyspark
df = spark.read.csv('data.csv', header=True, sep=",")
df.limit(20).toPandas()  

## showing dataframe with display function 
display(df)


## check dataframe shape 
print((df.count(), len(df.columns)))


## check the column list
df.columns  

## checking the datatypes of each of the columns
df.dtypes  

## Renaming the columns 
df = df.withColumnRenamed("product code", "PRODUCT_CODE")\
    .withColumnRenamed("products", "PRODUCTS")










