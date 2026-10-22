"""Start the exam environment's Spark worker from the installed package directory.

Windows/Python 3.9 can fail to read PySpark 4.0.4 error resources from
pyspark.zip. This process-local workaround leaves installed files unchanged.
"""

import runpy
import sys
import sysconfig


if __name__ == "__main__":
    sys.path.insert(0, sysconfig.get_path("purelib"))
    runpy.run_module("pyspark.worker", run_name="__main__")
