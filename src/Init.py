import sys

from util.SparkSessionProvider import SparkSessionProvider
from model import Process

from conf import AppConf

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: Joins", file=sys.stderr)
        sys.exit(-1)

appConf = AppConf(sys.argv[0], sys.argv[1], sys.argv[2], sys.argv[3])
spark = SparkSessionProvider
sparkSession = spark.createsparksession()
try:
  processObj = Process
  processObj.apply(appConf, sparkSession)
except Exception as e:
  raise e

