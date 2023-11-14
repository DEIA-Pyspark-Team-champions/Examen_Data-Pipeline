from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col, when, mean

# Fonction contenant le code PySpark pour charger les données
def load_data():
    spark = SparkSession.builder.appName("LoadData").getOrCreate()
    airports = spark.read.csv("/home/gomes/Documents/cours/conceptiin pleippleain/examen/archive/airports.csv", header=True, inferSchema=True)
    df_flight = spark.read.csv("/home/gomes/Documents/cours/conceptiin pleippleain/examen/archive/flights.csv", header=True, inferSchema=True)
    df_raw_flight = spark.read.csv("/home/gomes/Documents/cours/conceptiin pleippleain/examen/archive/raw-flight-data.csv", header=True, inferSchema=True)
    
    dat = df_raw_flight.intersect(df_flight)
    sans_doublons = dat.dropDuplicates()
    df = sans_doublons.na.drop()

    df_filtre = df.filter((df["DepDelay"].cast("int").isNotNull()) & ((df["DepDelay"] >= 0) | (df["DepDelay"] < 0)) &
                          (df["ArrDelay"].cast("int").isNotNull()) & ((df["ArrDelay"] >= 0) | (df["ArrDelay"] < 0)))

# Fonction pour appliquer StringIndexer
def apply_string_indexer():
    stringIndexer = StringIndexer(inputCol="Carrier", outputCol="Carrier_to_numérique")
    model = stringIndexer.fit(df_filtre)
    data = model.transform(df_filtre)

# Fonction pour créer VectorAssembler
def create_vector_assembler():
    input_cols = [col for col in df.columns if col not in ['Carrier']]
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
    data = assembler.transform(data)

# Fonction pour entraîner le modèle de régression linéaire pour le départ
def train_linear_regression_depart():
    train_ratio = 0.8
    test_ratio = 0.2
    train, test = data.randomSplit([train_ratio, test_ratio])
    lr = LinearRegression(featuresCol="features", labelCol="DepDelay")
    model = lr.fit(train)
    predictions_Dep = model.transform(test)
    predictions_Dep = predictions_Dep.withColumnRenamed("prediction", "prediction_Depp")

# Fonction pour entraîner le modèle de régression linéaire pour l'arrivée
def train_linear_regression_arrivee():
    lr_2 = LinearRegression(featuresCol="features", labelCol="ArrDelay")
    model = lr_2.fit(train)
    predictions_Arr = model.transform(test)
    predictions_Arr = predictions_Arr.withColumnRenamed("prediction", "prediction_Arri")

# Fonction pour évaluer le modèle pour le départ
def evaluate_linear_regression_depart():
    evaluator = RegressionEvaluator(labelCol="DepDelay", predictionCol="prediction_Depp", metricName="rmse")
    rmse = evaluator.evaluate(predictions_Dep)

# Fonction pour évaluer le modèle pour l'arrivée
def evaluate_linear_regression_arrivee():
    evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction_Arri", metricName="rmse")
    rmse = evaluator.evaluate(predictions_Arr)

# Fonction pour supprimer les colonnes inutiles
def remove_columns():
    predictions_Dep = predictions_Dep.drop(*["DayofMonth", "DayOfWeek", "Carrier", "Carrier_to_numérique", "features"])

# Fonction pour calculer la moyenne des prédictions totales
def calculate_average():
    prediction = predictions_Dep.join(predictions_Arr.select("ArrDelay", "prediction_Arri"), on="ArrDelay", how="left")
    prediction_Total = prediction.withColumn("prediction_total", col("prediction_Depp") + col("prediction_Arri"))
    moyenne = prediction_Total.select(mean("prediction_total")).collect()[0][0]
    prediction_Total = prediction_Total.withColumn("retard", when((col("prediction_total") < moyenne), 1).otherwise(0))

# Paramètres par défaut du DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 1, 1),
    'schedule_interval': '@daily',  # Planification quotidienne
}

# Créez un objet DAG
dag = DAG('10_Tâches', default_args=default_args, catchup=False)

# Définissez 10 tâches pour exécuter les étapes du code PySpark
tasks = [
    PythonOperator(task_id='load_data', python_callable=load_data, dag=dag),
    PythonOperator(task_id='apply_string_indexer', python_callable=apply_string_indexer, dag=dag),
    PythonOperator(task_id='create_vector_assembler', python_callable=create_vector_assembler, dag=dag),
    PythonOperator(task_id='train_linear_regression_depart', python_callable=train_linear_regression_depart, dag=dag),
    PythonOperator(task_id='train_linear_regression_arrivee', python_callable=train_linear_regression_arrivee, dag=dag),
    PythonOperator(task_id='evaluate_linear_regression_depart', python_callable=evaluate_linear_regression_depart, dag=dag),
    PythonOperator(task_id='evaluate_linear_regression_arrivee', python_callable=evaluate_linear_regression_arrivee, dag=dag),
    PythonOperator(task_id='remove_columns', python_callable=remove_columns, dag=dag),
    PythonOperator(task_id='calculate_average', python_callable=calculate_average, dag=dag),
]

# Définissez les dépendances entre les tâches
for i in range(1, len(tasks)):
    tasks[i] >> tasks[i-1]

if __name__ == "__main__":
    dag.cli()

