from pyspark.sql import SparkSession

class SparkSessionProvider:
    def createsparksession(self):
        return SparkSession.builder.appName("pyspark-example").getOrCreate