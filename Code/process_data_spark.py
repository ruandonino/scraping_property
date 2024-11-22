from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when
from datetime import date
from pyspark.sql.functions import regexp_extract, current_date, date_format, expr, year, current_date, to_date

def read_df(spark, read_path):
    # Read the Parquet file into a DataFrame
    df_data_stock = spark.read.parquet(read_path)
    return df_data_stock
def process_data_spark():
    # Initialize a SparkSession
    spark = SparkSession.builder \
        .appName("DropEmptyAndTrimWhitespace") \
        .getOrCreate()

    today = date.today()
    website = 'imovelweb'
    # Define the path to the Parquet file on Google Cloud Storage
    read_file_path = f"gs://python_files_property/outputs_extracted_data/{website}/{today}/{website}-{today}.parquet"
    # Read the Parquet file into a DataFrame
    df_data_stock = read_df(spark,read_file_path)

    # Transform the DataFrame
    #df_data_stock = transform_df(df_data_stock)

    output_path = f"gs://python_files_stock2/outputs_processed_data/processed_data_{today}"
    df_data_stock.write.mode('overwrite').parquet(output_path)
    return df_data_stock




if __name__ == "__main__":
    result_data =process_data_spark()