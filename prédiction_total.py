from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col
from pyspark.sql.functions import when, col
from pyspark.sql.functions import mean


# 1. Créez une session Spark
spark = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()
# 2. Chargez le fichier CSV dans un DataFrame


df_flight = spark.read.csv("/home/massinissa/Téléchargements/predection/flights.csv" , header=True, inferSchema=True)

df_raw_flight = spark.read.csv("/home/massinissa/Téléchargements/predection/raw-flight-data.csv" , header=True, inferSchema=True)

dat=df_raw_flight.intersect(df_flight)


sans_doublons=dat.dropDuplicates()

df=sans_doublons.na.drop()

df_filtre = df.filter((df["DepDelay"].cast("int").isNotNull()) & (df["DepDelay"] >= 0) | (df["DepDelay"] < 0 ) &
                      (df["ArrDelay"].cast("int").isNotNull()) & (df["ArrDelay"] >= 0) |  (df["ArrDelay"] < 0))
   
stringIndexer = StringIndexer(inputCol="Carrier", outputCol="Carrier_to_numérique")

# Ajustez le modèle de conversion aux données
model = stringIndexer.fit(df_filtre )

# Transformez les données pour ajouter la colonne numérique
data = model.transform(df_filtre )

input_cols = [col for col in df.columns if col not in ['Carrier']]

# Créez le VectorAssembler
assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

# Transformez le DataFrame en ajoutant la colonne "features"
data= assembler.transform(data)

train_ratio = 0.8  # 80% pour le jeu d'entraînement
test_ratio = 0.2   # 20% pour le jeu de test

# Utilisez la méthode randomSplit pour effectuer la division
train, test = data.randomSplit([train_ratio, test_ratio])

# Le DataFrame 'train' contient 80% des données
# Le DataFrame 'test' contient 20% des données

# Vous pouvez également afficher la taille de chaque jeu de données
print("Taille du jeu d'entraînement : ", train.count())
train.show()


#print("Taille du jeu de test : ", test.count())
#test.show ()
#lineaire regression pour le départ
lr = LinearRegression(featuresCol="features", labelCol="DepDelay")

# Utilisez la méthode .fit() pour ajuster le modèle aux données d'entraînement
model = lr.fit(train)

predictions_Dep = model.transform(test)

predictions_Dep = predictions_Dep.withColumnRenamed("prediction", "prediction_Depp")

predictions_Dep.show()

#lineaire regression pour l'arrivé
lr_2 = LinearRegression(featuresCol="features", labelCol="ArrDelay")

model = lr_2.fit(train)

predictions_Arr = model.transform(test)
predictions_Arr = predictions_Arr.withColumnRenamed("prediction", "prediction_Arri")
predictions_Arr.show()

# Utilisez RegressionEvaluator pour évaluer le modèle pour le départ
evaluator = RegressionEvaluator(labelCol="DepDelay", predictionCol="prediction_Depp", metricName="rmse")
rmse = evaluator.evaluate(predictions_Dep)

# Affichez la racine carrée de l'erreur quadratique moyenne (RMSE)
print(f"RMSE_Dep : {rmse}")

# Utilisez RegressionEvaluator pour évaluer le modèle pour l'arrivé
evaluator = RegressionEvaluator(labelCol="ArrDelay", predictionCol="prediction_Arri", metricName="rmse")
rmse = evaluator.evaluate(predictions_Arr)

# Affichez la racine carrée de l'erreur quadratique moyenne (RMSE)
print(f"RMSE_Arr : {rmse}")

#suprimer colones DayofMonth|DayOfWeek|Carrier du predictions_Dep

predictions_Dep = predictions_Dep.drop(*["DayofMonth","DayOfWeek","Carrier","Carrier_to_numérique","features"])

prediction = predictions_Dep.join(predictions_Arr.select("ArrDelay", "prediction_Arri"), on="ArrDelay", how="left")

#prediction.show()

prediction_Total = prediction.withColumn("prediction_total", col("prediction_Depp") + col("prediction_Arri"))

prediction_Total.show()

moyenne = prediction_Total.select(mean("prediction_total")).collect()[0][0]

# Affichage de la moyenne
print("le retard moyen est :", moyenne)

prediction_Total = prediction_Total.withColumn("retard", when( (col("prediction_total") < moyenne), 1).otherwise(0))

prediction_Total.show()
prediction_Total.printSchema()



	
