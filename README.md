# mapr-dev3600

Solutions to MapR labs 3600, using Python.
Scripts are to be executed with a fresh, clean installation of the [MapR sandbox virtual machine](https://www.mapr.com/products/mapr-sandbox-hadoop/download) (v5.2.0).

* http://learn.mapr.com/dev-360-apache-spark-essentials
* http://learn.mapr.com/dev-361-build-and-monitor-apache-spark-applications
* http://learn.mapr.com/dev-362-create-data-pipelines-using-apache-spark

The required data files can be downloaded from the MapR training website after registering for one of these trainings.

### PySpark

To launch a pyspark shell in the terminal:

    pyspark --master local[2]

To execute `script.py`:

    spark-submit --master local[2] script.py

### How to start

After setting up a fresh, clean installation of the [MapR sandbox virtual machine](https://www.mapr.com/products/mapr-sandbox-hadoop/download)
you can copy the necessary files to the vm using the `scp_files_to_vm.R` script.
_You need to have [R](https://www.r-project.org/) installed to run this script._
Make sure to check the settings at the start of this script and modify as required!

Next you can run the script files in the `scripts` folder using `spark-submit`.

#### Enable native hadoop libraries

To enable PySpark to use the native hadoop libraries, you will have to modify
the `LD_LIBRARY_PATH` environment variable to include the path to these
libraries. The `spark_hadoop_native.sh` script sets appropriate value for the
MapR sandbox version 5.2.0.
