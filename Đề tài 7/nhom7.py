#Khai báo thư viện và đọc file csv
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,to_date,avg,regexp_extract,when,year,count,sum,round
spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header','true').load('D:/Big Data/Đề tài 7/king-county-house-sales.csv')

# print("Cau 3.2")
# tong_so_nha_ban = data.count()
# print("So nha duoc ban ra la: ",tong_so_nha_ban)

# print("Cau 3.3")
# So_vung_duoc_liet_ke=data.select("zipcode").distinct().count()
# print("Tong cac phan vung la",So_vung_duoc_liet_ke)
# data.select('zipcode').distinct().show(n=So_vung_duoc_liet_ke)

#tạo biểu thức chính quy có dạng [0-9]+[.]?[0-9]*[eE][+]([0-9]+
print("Cau 3.4")
data = data.withColumn("sqft_living", data["sqft_living"].cast("int")) \
           .withColumn("floors", data["floors"].cast("int")) \
         .withColumn("price", 
                       when(data["price"].rlike("[0-9]+[.]?[0-9]*[eE][+]([0-9]+)"), 
                            (regexp_extract("price", "([0-9]+[.]?[0-9]*)[eE][+]([0-9]+)", 1).cast("double") * 10**6).cast("int")
                           ).otherwise(data["price"].cast("int"))
                      ) 
# #           .withColumn("price", 
# #                        when(data["price"].rlike("[0-9]+[.]?[0-9]*[eE][+]([0-9]+)"), 
# #                             (regexp_extract("price", "([0-9]+[.]?[0-9]*)[eE][+]([0-9]+)", 1).cast("double") * pow(10,regexp_extract("price", "([0-9]+[.]?[0-9]*)[eE][+]([0-9]+)", 2).cast("int"))).cast("int")
# #                            ).otherwise(data["price"].cast("int"))
# #                       ) 
# #Kiểm tra lại schema của DataFrame sau khi thay đổi
# print("Schema sau khi thay doi:")
# data.printSchema()
# data.show()

# print("Cau 3.5")
# gia_nha_theo_vung = data.groupBy('zipcode').agg(avg('price').alias('avg_price'))
# print("Gia nha theo vung:")
# gia_nha_theo_vung.show(n=So_vung_duoc_liet_ke)


print("Cau 3.6")
unique_data = data.dropDuplicates(['id'])
filtered_data = unique_data.filter((col("bedrooms") >= 3) & (col("yr_built") > 2000))
# print("so luong nha thoa man dieu kien",filtered_data.count())
filtered_data.show(20)
print("Tong so luong nha thoa man: ",filtered_data.count())
gia_trung_binh = filtered_data.groupBy("floors").agg(avg("price").alias("gia_nha_trung_binh"))
gia_trung_binh.show()


# print("Cau 3.7")
# tim_nha_ba_phong_ngu= data.filter((col("bedrooms")<=3) & (col("price")<=250000))
# liet_ke = tim_nha_ba_phong_ngu.groupBy("floors").agg(count('*').alias('tong_so_nha'),avg('price').alias('gia_nha_trung_binh'))
# # liet_ke_rounded = liet_ke.withColumn("gia_nha_trung_binh", round(liet_ke["gia_nha_trung_binh"], 2))
# # liet_ke_rounded.show()
# liet_ke.show()

# print("Cau 3.8")
# # Chuyển đổi cột date sang định dạng ngày tháng
# data = data.withColumn("sale_date", to_date(data["date"], "yyyyMMdd'T'HHmmss"))
# unique_data = data.dropDuplicates(['id'])
# # Trích xuất năm từ cột ngày và lọc ra những nhà được đăng bán vào năm 2015
# nha_ban_2015 = data.filter(year(unique_data["sale_date"]) == 2015)
# # print("so luong nha ban duoc",nha_ban_2015)
# #dayofmonth(data["sale_date"])
# n=nha_ban_2015.count()
# # Hiển thị dữ liệu
# nha_ban_2015.show(n)


# print("Cau 3.9")
# unique_data = data.dropDuplicates(['id'])
# result = (unique_data
#           .groupBy('bedrooms')
#           .agg(
#               sum(when(unique_data['floors'] == 1, 1).otherwise(0)).alias('floors_1'),
#               sum(when(unique_data['floors'] == 2, 1).otherwise(0)).alias('floors_2'),
#               sum(when(unique_data['floors'] == 3, 1).otherwise(0)).alias('floors_3')
#           )
#           .orderBy('bedrooms')
#          )
# result.show()

# print("Cau 3.10")
# # Tạo cột mới với tên là "size_category"
# data = data.withColumn("size_category", 
#                        when(data["sqft_living"] <= 1000, "small")
#                        .when((data["sqft_living"] > 1000) & (data["sqft_living"] < 2000), "medium")
#                        .otherwise("large")
#                       )
# n =data.count()
# # Hiển thị dữ liệu với cột mới đã tạo
# data.show(n)


#print("Cau 3.9")
# data = data.withColumn("bathrooms", data["bathrooms"].cast("float"))
#  Làm tròn giá trị của cột "bathrooms" theo yêu cầu
#  tạo cột bathrooms_rounded
# data = data.withColumn("bathrooms_rounded", 
#                        when(data["bathrooms"] - data["bathrooms"].cast("int") >= 0.5, data["bathrooms"].cast("int") + 1)
#                        .otherwise(data["bathrooms"].cast("int"))
#                       )

# Hiển thị dữ liệu đã làm tròn
# data.select("bathrooms", "bathrooms_rounded").show()
# # Xoay cột floors theo bedrooms
# pivot_bedroom_data = data.groupBy('bathrooms_rounded').pivot('floors').count()
# # Xoay cột floors theo bathrooms
# pivot_bathrooms_data = data.groupBy('bedrooms').pivot('floors').count()

#  Hiển thị kết quả
#  pivot_bedroom_data.show()
# pivot_bathrooms_data.show()