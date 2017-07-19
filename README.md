#### Install Anaconda
```bash
bash Anaconda-2.2.0-Linux-x86_64.sh (point the installation to ~/anaconda)
export PATH=~/anaconda/bin:$PATH
```
#### Zip anaconda installation
```bash
cd ~/anaconda
zip -r anaconda.zip .
mv anaconda.zip ~/ (moving the zip back to home directory)
Copy ~/anaconda.zip to HDFS
```

#### Set Paths
* Set PYSPARK_PYTHON
```bash
export PYSPARK_PYTHON=./anaconda/bin/python
```
* You also need to set the PYSPARK_PYTHON env variable on the executor nodes. Pass:
```bash
--conf spark.executorEnv.PYSPARK_PYTHON=./anaconda/bin/python
```
to spark-submit
* If you are running in cluster mode you also have to export PYSPARK_PYTHON? on the application master so also add:
```bash
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./anaconda/bin/python
```

#### Running with Anaconda
* Add the location of your zipped Anaconda on HDFS to your PySpark command using the "--archives" option. For example, to compute the value pi, run the following script:
* Add any configs you want via normal spark configuration: http://twiki.corp.yahoo.com/view/Grid/SparkOnYarnProduct#Spark_Configs
* Run it: 
```bash
$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --queue default \
  --num-executors 5 \
  --driver-memory 2G \
  --conf spark.executorEnv.PYSPARK_PYTHON=./anaconda/bin/python \
  --archives 'hdfs:///user/USER/anaconda.zip#anaconda' \
  pi.py \
  10
```
