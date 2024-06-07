from pyspark import SparkContext, SparkConf
import sys
import os
from pyspark.sql import SparkSession
#định nghĩa cấu trúc của dataframe
from pyspark.sql.types import StructType
from pyspark.sql.functions import input_file_name, sum as Fsum, when, col
import urllib.parse

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .appName("streaming") \
    .getOrCreate()

# OFF để giảm bớt thông báo log
spark.sparkContext.setLogLevel("OFF")
# Khởi tạo biến để tích lũy kết quả từ các batch trước đó
# total_matches_accumulator = spark.sparkContext.accumulator(0)

# Hàm để Inferschema từ đường dẫn tệp
def infer_schema(file_path):
    sample_df = spark.read.option("header", "true").csv(file_path)
    return sample_df.schema

# Đọc dữ liệu streaming từ folder chứa các tệp
streaming_df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .schema(StructType([])) \
    .load("D:/Big Data/Streaming")

# Hàm để xử lý mỗi batch 
def process_batch(df, batch_id):

    # đường dẫn tệp từ DataFrame và giải mã
    encoded_file_path = df.select(input_file_name()).first()[0]
    #để giải mã đường dẫn tệp tin nếu đường dẫn được mã hóa (ví dụ, nếu có các ký tự đặc biệt được mã hóa trong đường dẫn).
    file_path = urllib.parse.unquote(encoded_file_path)

    # Lấy schema sử dụng đường dẫn tệp
    schema = infer_schema(file_path)

    # Đọc tệp CSV với schema vừa tạo
    df = spark.read.option("header", "true").schema(schema).csv(file_path)
    
    # Lọc các trận thắng của đội khách
    away_wins_df = df.filter(df["FTR"] == "A")
    
    # Group by AwayTeam và đếm số trận thắng khách
    away_wins_count_df = away_wins_df.groupBy("AwayTeam") \
                                     .agg(Fsum(when(col("FTR") == "A", 1).otherwise(0)).alias("AwayWins"),
                                          Fsum("AST").alias("TotalShotsOnTarget"))
    
    # Chọn ra 3 đội có số trận thắng khách nhiều nhất
    top_3_teams_df = away_wins_count_df.orderBy(col("AwayWins").desc()).limit(3)
    
    # Hiển thị kết quả
    print("Top 3 teams with most away wins and their total shots on target:")
    top_3_teams_df.show()

# Bắt đầu truy vấn streaming
query = streaming_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Chờ truy vấn streaming kết thúc
query.awaitTermination()
