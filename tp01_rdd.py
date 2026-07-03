
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TP01 - RDD et paresse")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

sc = spark.sparkContext     # point d'entrée pour les RDD

nombres = sc.parallelize(range(1, 21), numSlices=4)

# À compléter : afficher le nombre de partitions
print("Partitions :", nombres.getNumPartitions())

# À compléter : ramener tous les éléments vers le driver (action)
print("Contenu :", nombres.collect())

#Question : `parallelize` est-il une transformation ou une action ? Et `collect` ?

### 2. map et filter

# Calculez le carré de chaque nombre, puis ne gardez que les carrés pairs. Enchaînez les
# transformations, puis déclenchez le calcul avec une action.


carres = nombres.map(lambda x: x * x)
carres_pairs = carres.filter(lambda x: x % 2 == 0)

# À compléter : récupérer le résultat (action)
print("Carres pairs :", carres_pairs.collect())


# À ce stade, demandez-vous : à quel moment précis Spark a-t-il réellement calculé quelque chose ?

### 3. Observer la paresse

# Insérez un effet de bord (un `print`) dans la fonction passée à `map`, puis construisez la chaîne
# de transformations SANS appeler d'action. Constatez que rien ne s'affiche.


def trace(x):
    print("  -> je calcule", x)     # effet de bord visible
    return x * 10

paresseux = nombres.map(trace)
print("La transformation est definie, mais rien ne s'est encore affiche.")

# À compléter : maintenant, déclenchez une action et observez les traces apparaître
resultat = paresseux.collect()
print("Resultat :", resultat)

# Notez l'ordre des messages dans la console : la phrase "rien ne s'est encore affiche" doit sortir
# AVANT les lignes "-> je calcule".

### 4. flatMap : aplatir

# `map` renvoie un élément par élément d'entrée. `flatMap` renvoie zéro, un ou plusieurs éléments
# par élément d'entrée, puis aplatit le tout. Comparez les deux sur des phrases.

phrases = sc.parallelize([
    "le taxi jaune roule",
    "le taxi attend le client",
])

# map : une liste de mots par phrase (resultat imbrique)
par_map = phrases.map(lambda p: p.split(" "))

# flatMap : tous les mots a plat
par_flatmap = phrases.flatMap(lambda p: p.split(" "))

print("map      :", par_map.collect())
print("flatMap  :", par_flatmap.collect())


### 5. Word count avec reduceByKey

# Assemblez le pipeline classique du word count : aplatir en mots, transformer chaque mot en couple
# `(mot, 1)`, puis sommer par clé avec `reduceByKey`.


texte = sc.parallelize([
    "le taxi jaune roule dans la ville",
    "le client appelle le taxi jaune",
    "la ville dort le taxi roule",
])

comptage = (
    texte
    .flatMap(lambda ligne: ligne.split(" "))   # aplatir en mots
    .map(lambda mot: (mot, 1))                  # À compléter si besoin : couple (mot, 1)
    .reduceByKey(lambda a, b: a + b)               # À compléter : sommer par clé
)

# Trier par frequence decroissante et afficher le top 5
top = comptage.sortBy(lambda couple: couple[1], ascending=False).take(5)
for mot, n in top:
    print(f"{mot:10s} {n}")


### 6. Du DataFrame taxi vers le RDD

# Chargez le Parquet taxi en DataFrame, puis descendez sur son RDD sous-jacent (`df.rdd`) pour
# compter à la main le nombre de courses par nombre de passagers. C'est exactement un word count,
# mais la clé est `passenger_count`.


chemin = "data/datasets/yellow_tripdata_2024-01.parquet"
df = spark.read.parquet(chemin).select("passenger_count")

comptage_passagers = (
    df.rdd
    .map(lambda row: (row["passenger_count"], 1))   # couple (nb_passagers, 1)
    .reduceByKey(lambda a, b: a + b)                    # À compléter : sommer par clé
    .sortByKey()
)

for nb_passagers, n in comptage_passagers.collect():
    print(nb_passagers, "->", n)