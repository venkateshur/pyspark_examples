

def textReader(spark, inputPath):
    return spark.read.textFile(inputPath)

def csvReader(spark, inputPath):
    return spark.read.option("header", "true").csv(inputPath)
