# created rdd for the data

rdd1 = sc.textFile("/home/root1/futurense_hadoop-pyspark/labs/dataset/weather/weather_data.txt")

# made the data consistent by substituting spaces with comma

import re

rdd2= rdd1.map(lambda row: re.sub(r'\s+',',',x))

# finding max temprature

max_temp = rdd2.map(lambda row: float(row.split(',')[5])).max()

# finding min_temp

min_temp = rdd2.map(lambda row: float(row.split(',')[5])).min()