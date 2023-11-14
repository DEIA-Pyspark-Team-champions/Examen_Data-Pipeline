from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col, when, mean

# Définissez la fonction contenant votre code
def run_spark_job():
    # 1. Créez une session Spark
    spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()

    # 2. Chargez les fichiers CSV dans des DataFrames
    airports = spark.read.csv("/home/gomes/Documents/cours/conception pleine/examen/archive/airports.csv", header=True, inferSchema=True)
    df_flight = spark.read.csv("/home/gomes/Documents/cours/conception pleine/examen/archive/flights.csv", header=True, inferSchema=True)
    df_raw_flight = spark.read.csv("/home/gomes/Documents/cours/conception pleine/examen/archive/raw-flight-data.csv", header=True, inferSchema=True)

    dat = df_raw_flight.intersect(df_flight)
    sans_doublons = dat.dropDuplicates()
    df = sans_doublons.na.drop()

    df_filtre = df.filter(
        (df["DepDelay"].cast("int").isNotNull()) & ((df["DepDelay"] >= 0) | (df["DepDelay"] < 0)) &
        (df["ArrDelay"].cast("int").isNotNull()) & ((df["ArrDelay"] >= 0) | (df["ArrDelay"] < 0))
    )

    stringIndexer = StringIndexer(inputCol="Carrier", outputCol="Carrier_to_numérique")
    model = stringIndexer.fit(df_filtre)
    data = model.transform(df_filtre)

    input_cols = [col for col in df.columns if col not in ['Carrier']]
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    data = assembler.transform(data)

    train_ratio = 0.8
    test_ratio = 0.2
    train, test = data.randomSplit([train_ratio, test_ratio])

    lr = LinearRegression(featuresCol="features", labelCol="DepDelay")
    model = lr.fit(train)
    predictions_Dep = model.transform(test)
    predictions_Dep = predictions_Dep.withColumnRenamed("prediction", "prediction_Depp")

    lr_2 = LinearRegression(featuresCol="features", labelCol="ArrDelay")
    model = lr_2.fit(train)
    predictions_Arr = model.transform(test)
    predictions_Arr = predictions_Arr.withColumnRenamed("prediction", "prediction_Arri")

    evaluator = RegressionEvaluator(labelCol="DepDelay", predictionCol="prediction_Depp", metricName="rmse")
    rmse = evaluator.evaluate(predictions_Dep)

    evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction_Arri", metricName="rmse")
    rmse = evaluator.evaluate(predictions_Arr)

    predictions_Dep = predictions_Dep.drop(*["DayofMonth", "DayOfWeek", "Carrier", "Carrier_to_numérique", "features"])
    prediction = predictions_Dep.join(predictions_Arr.select("ArrDelay", "prediction_Arri"), on="ArrDelay", how="left")
    prediction_Total = prediction.withColumn("prediction_total", col("prediction_Depp") + col("prediction_Arri"))
    moyenne = prediction_Total.select(mean("prediction_total")).collect()[0][0]
    prediction_Total = prediction_Total.withColumn("retard", when((col("prediction_total") < moyenne), 1).otherwise(0))
    prediction_Total = prediction_Total.coalesce(1)
    prediction_Total.write.format("csv").option("header", "true").mode("overwrite").save("/home/gomes/Documents/cours/conception pleine/examen/output.csv")

# Définissez les paramètres de votre DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 1, 1),
    'schedule_interval': '@daily',  # Planification quotidienne
}

# Créez un objet DAG
dag = DAG('your_airflow_dag_id', default_args=default_args, catchup=False)

# Définissez une tâche qui exécute votre fonction
run_task = PythonOperator(
    task_id='run_spark_job',
    python_callable=run_spark_job,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()

