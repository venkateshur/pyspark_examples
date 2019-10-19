
def csvWriter(inDf, outputPath):
    inDf.write.mode("overwrite").save(outputPath)