from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import json


spark = SparkSession.builder.appName("Desafio_XPTO").getOrCreate()

df = spark.read.json('s3://raw'+'*.json')

df = df.withColumn("tweet_text", decode(encode("tweet_text", 'ISO-8859-1'), 'UTF-8'))

emocaopos = [":D",":)",":]",":-)",":P",":p",":v",": )"]
emocaoneg = [":(",":[",":{"]

todas_emocoes = [':D', ':)', ':]', ':-)', ':P', ':p', ':v', ': )',':(', ':[', ':{']

df_emocao = df.withColumn(":D", locate(":D", col("tweet_text")))\
                .withColumn(":)", locate(":)", col("tweet_text")))\
                    .withColumn(":]", locate(":]", col("tweet_text")))\
                        .withColumn(":-)", locate(":-)", col("tweet_text")))\
                            .withColumn(":P", locate(":P", col("tweet_text")))\
                                .withColumn(":p", locate(":p", col("tweet_text")))\
                                    .withColumn(":v", locate(":v", col("tweet_text")))\
                                        .withColumn(": )", locate(": )", col("tweet_text")))\
                                            .withColumn(":(", locate(":(", col("tweet_text")))\
                                                .withColumn(":[", locate(":[", col("tweet_text")))\
                                                    .withColumn(":{", locate(":{", col("tweet_text")))


df_simbolos = df_emocao.withColumn('simbolo', least(*[(when(col(c).isNull() | (col(c) == 0), "None").otherwise(c))for c in df_emocao.columns[3:]]))

df_simbolos = df_simbolos.drop(*todas_emocoes)

df_completa = df_simbolos.withColumn('Sentimento', when(df_simbolos.simbolo.isin(emocaopos), 'positivo')\
                                                .when(df_simbolos.simbolo.isin(emocaoneg), 'negativo')\
                                                    .otherwise("neutra"))

df_completa.write.partitionBy("Year","Month","Day").parquet("s3://ref", mode = 'overwrite')
