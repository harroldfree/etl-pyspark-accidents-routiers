from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Projet_ONISR") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Shema pour le fichier caract-2024.csv
schema_caract = StructType([
    StructField("Num_Acc", LongType(), True),
    StructField("jour", IntegerType(), True),
    StructField("mois", IntegerType(), True),
    StructField("an", IntegerType(), True),
    StructField("hrmn", StringType(), True),
    StructField("lum", IntegerType(), True),
    StructField("dep", StringType(), True),
    StructField("com", StringType(), True),
    StructField("agg", IntegerType(), True),
    StructField("int", IntegerType(), True),
    StructField("atm", IntegerType(), True),
    StructField("col", IntegerType(), True),
    StructField("adr", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True)
])

# Shema pour le fichier lieux-2024.csv
schema_lieux = StructType([
    StructField("Num_Lieu", LongType(), True),
    StructField("nom", StringType(), True),
    StructField("adr", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("dep", StringType(), True),
    StructField("com", StringType(), True),
    StructField("agg", IntegerType(), True),
    StructField("int", IntegerType(), True),
    StructField("atm", IntegerType(), True),
    StructField("col", IntegerType(), True),
    StructField("hrmn", StringType(), True),
    StructField("lum", IntegerType(), True),
    StructField("jour", IntegerType(), True),
    StructField("mois", IntegerType(), True),
    StructField("an", IntegerType(), True)
])

# Shema pour le fichier vehicules-2024.csv
schema_vehicules = StructType([
    StructField("Num_Acc", LongType(), True),
    StructField("id_vehicule", StringType(), True),
    StructField("num_veh", StringType(), True),
    StructField("senc", IntegerType(), True),
    StructField("catv", IntegerType(), True),
    StructField("obs", IntegerType(), True),
    StructField("obsm", IntegerType(), True),
    StructField("choc", IntegerType(), True),
    StructField("manv", IntegerType(), True),
    StructField("motor", IntegerType(), True),
    StructField("occutc", IntegerType(), True)
])

# Shema pour le fichier usagers-2024.csv
schema_usagers = StructType([
    StructField("Num_Usager", LongType(), True),
    StructField("Num_Veh", LongType(), True),
    StructField("Num_Acc", LongType(), True),
    StructField("Num_Lieu", LongType(), True),
    StructField("nom", StringType(), True),
    StructField("dep", StringType(), True),
    StructField("com", StringType(), True),
    StructField("agg", IntegerType(), True),
    StructField("int", IntegerType(), True),
    StructField("atm", IntegerType(), True),
    StructField("col", IntegerType(), True),
    StructField("hrmn", StringType(), True),
    StructField("lum", IntegerType(), True),
    StructField("jour", IntegerType(), True),
    StructField("mois", IntegerType(), True),
    StructField("an", IntegerType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("adr", StringType(), True),        
])  

caract = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .schema(schema_caract) \
    .csv("data/caract-2024.csv")

caract.printSchema()

caract.show(5)

print(caract.columns)

lieux = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .csv("data/lieux-2024.csv")

vehicules = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .csv("data/vehicules-2024.csv")

usagers = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .csv("data/usagers-2024.csv")

print("Accidents :", caract.count())
print("Lieux :", lieux.count())
print("Véhicules :", vehicules.count())
print("Usagers :", usagers.count())

# Nettoyage des données

print("Doublons caract :", caract.count() - caract.dropDuplicates().count())
print("Doublons lieux :", lieux.count() - lieux.dropDuplicates().count())
print("Doublons vehicules :", vehicules.count() - vehicules.dropDuplicates().count())
print("Doublons usagers :", usagers.count() - usagers.dropDuplicates().count())

caract = caract.dropDuplicates()
lieux = lieux.dropDuplicates()
vehicules = vehicules.dropDuplicates()
usagers = usagers.dropDuplicates()

print("\nValeurs manquantes dans caract")

caract.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in caract.columns
]).show()


#   Écriture de la couche Silver

# caract.write.mode("overwrite").parquet("output/silver/caract")
# lieux.write.mode("overwrite").parquet("output/silver/lieux")
# vehicules.write.mode("overwrite").parquet("output/silver/vehicules")
# usagers.write.mode("overwrite").parquet("output/silver/usagers")

print("Couche Silver enregistrée avec succès.")







spark.stop()