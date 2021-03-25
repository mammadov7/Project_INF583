from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from nltk.stem.snowball import SnowballStemmer

spark = SparkSession\
  .builder \
  .appName("Parquet_is_life") \
  .getOrCreate()

if __name__ == "__main__":
    file_name = './datasets/NoFilterEnglish2020-02-01.json'

    data = spark.read.format('json').options(header='true', inferSchema='true') \
                .load(file_name)

    print(file_name+ " has been loaded!")

    for i in range(2,8):
        file_name = './datasets/NoFilterEnglish2020-02-0{}.json'.format(i)
        data_loc = spark.read.format('json').options(header='true', inferSchema='true') \
                        .load(file_name)
        
        print(file_name+ " has been loaded!")
        
        data = data.union(data_loc)
    
    data.select('text').show(1, False)

    temp_df = data.repartition(47)

    temp_df.write.parquet('Parquets/tweets.parquet')

    df = spark.read.parquet('Parquets/tweets.parquet')
    df.columns