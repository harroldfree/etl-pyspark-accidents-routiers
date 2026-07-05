"""
Pipeline ETL pour les données d'accidents routiers ONISR
Traitement de 4 fichiers CSV en couches Bronze -> Silver
"""

import sys
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, count, when, regexp_replace, avg, split, to_date,
    concat_ws, date_format, dense_rank, round as spark_round
)

sys.stdout.reconfigure(encoding="utf-8")

# ============================================================================
# CONFIGURATION
# ============================================================================

def init_spark():
    """Initialise la session Spark"""
    spark = SparkSession.builder \
        .appName("Projet_ONISR") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ============================================================================
# SCHEMAS
# ============================================================================

SCHEMAS = {
    "caract": StructType([
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
    ]),
    
    "lieux": StructType([
        StructField("Num_Acc", LongType(), True),
        StructField("catr", IntegerType(), True),
        StructField("voie", StringType(), True),
        StructField("v1", StringType(), True),
        StructField("v2", StringType(), True),
        StructField("circ", IntegerType(), True),
        StructField("nbv", IntegerType(), True),
        StructField("vosp", IntegerType(), True),
        StructField("prof", IntegerType(), True),
        StructField("pr", StringType(), True),
        StructField("pr1", StringType(), True),
        StructField("plan", IntegerType(), True),
        StructField("lartpc", StringType(), True),
        StructField("larrout", StringType(), True),
        StructField("surf", IntegerType(), True),
        StructField("infra", IntegerType(), True),
        StructField("situ", IntegerType(), True),
        StructField("vma", IntegerType(), True)
    ]),
    
    "vehicules": StructType([
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
    ]),
    
    "usagers": StructType([
        StructField("Num_Acc", LongType(), True),
        StructField("id_usager", StringType(), True),
        StructField("id_vehicule", StringType(), True),
        StructField("num_veh", StringType(), True),
        StructField("place", IntegerType(), True),
        StructField("catu", IntegerType(), True),
        StructField("grav", IntegerType(), True),
        StructField("sexe", IntegerType(), True),
        StructField("an_nais", IntegerType(), True),
        StructField("trajet", IntegerType(), True),
        StructField("secu1", IntegerType(), True),
        StructField("secu2", IntegerType(), True),
        StructField("secu3", IntegerType(), True),
        StructField("locp", IntegerType(), True),
        StructField("actp", StringType(), True),
        StructField("etatp", IntegerType(), True)
    ])
}

# ============================================================================
# FONCTIONS
# ============================================================================

def load_csv(spark, path, schema):
    """Charge un fichier CSV avec le schéma spécifié"""
    return spark.read \
        .option("header", True) \
        .option("sep", ";") \
        .schema(schema) \
        .csv(path)

def analyze_data(dataframes):
    """Analyse et affiche des statistiques sur les données"""
    print("\n" + "="*60)
    print("ANALYSE DES DONNÉES")
    print("="*60)
    
    for name, df in dataframes.items():
        print(f"\n{name.upper()}: {df.count()} lignes")
    
    print("\n" + "-"*60)
    print("DÉTECTION DES DOUBLONS")
    print("-"*60)
    for name, df in dataframes.items():
        duplicates = df.count() - df.dropDuplicates().count()
        print(f"{name}: {duplicates} doublons")

def clean_data(dataframes):
    """Supprime les doublons et les lignes sans clé de jointure (Num_Acc)"""
    print("\n" + "="*60)
    print("NETTOYAGE DES DONNÉES")
    print("="*60)
    cleaned = {}
    for name, df in dataframes.items():
        before = df.count()
        df = df.dropDuplicates().filter(col("Num_Acc").isNotNull())
        after = df.count()
        print(f"{name}: {before - after} lignes supprimées (doublons / Num_Acc manquant)")
        cleaned[name] = df
    return cleaned

def check_missing_values(df, name):
    """Vérifie les valeurs manquantes"""
    print(f"\nValeurs manquantes dans {name}:")
    df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).show()

def clean_outliers(dataframes):
    """Corrige les valeurs aberrantes connues du jeu de données ONISR"""
    print("\n" + "="*60)
    print("TRAITEMENT DES VALEURS ABERRANTES")
    print("="*60)

    # caract: coordonnées GPS stockées avec une virgule décimale ; hors-bornes -> null
    caract = dataframes["caract"] \
        .withColumn("lat", regexp_replace(col("lat"), ",", ".").cast(DoubleType())) \
        .withColumn("long", regexp_replace(col("long"), ",", ".").cast(DoubleType()))
    caract = caract.withColumn(
        "lat", when((col("lat") >= -22) & (col("lat") <= 51), col("lat"))
    ).withColumn(
        "long", when((col("long") >= -178) & (col("long") <= 168), col("long"))
    )

    # usagers: année de naissance hors bornes plausibles -> null (pas de suppression de ligne)
    usagers = dataframes["usagers"].withColumn(
        "an_nais",
        when((col("an_nais") >= 1900) & (col("an_nais") <= 2024), col("an_nais"))
    )

    # lieux: vitesse maximale autorisée négative ou irréaliste -> null
    lieux = dataframes["lieux"].withColumn(
        "vma",
        when((col("vma") >= 0) & (col("vma") <= 300), col("vma"))
    )

    dataframes = dict(dataframes)
    dataframes["caract"] = caract
    dataframes["usagers"] = usagers
    dataframes["lieux"] = lieux
    return dataframes

# ============================================================================
# ANALYSES (Gold layer)
# ============================================================================

def analyse_gravite_par_meteo(dfs):
    """Jointure : gravité des usagers selon la condition météo (atm) de l'accident"""
    print("\n" + "="*60)
    print("ANALYSE 1 (JOINTURE) - GRAVITÉ PAR CONDITION MÉTÉO")
    print("="*60)

    caract = dfs["caract"].select("Num_Acc", "atm")
    usagers = dfs["usagers"].select("Num_Acc", "grav")

    result = usagers.join(caract, on="Num_Acc", how="inner") \
        .groupBy("atm") \
        .agg(
            count("*").alias("nb_usagers"),
            spark_round(avg(when(col("grav") == 2, 1).otherwise(0)) * 100, 2).alias("pct_tues")
        ) \
        .orderBy(col("pct_tues").desc())

    result.show()
    return result

def analyse_accidents_par_heure_jour(dfs):
    """Agrégation : nombre d'accidents par jour de la semaine et heure"""
    print("\n" + "="*60)
    print("ANALYSE 2 (AGRÉGATION) - ACCIDENTS PAR JOUR ET HEURE")
    print("="*60)

    caract = dfs["caract"] \
        .withColumn("heure", split(col("hrmn"), ":").getItem(0).cast(IntegerType())) \
        .withColumn("date", to_date(concat_ws("-", col("an"), col("mois"), col("jour")))) \
        .withColumn("jour_semaine", date_format(col("date"), "E"))

    result = caract.groupBy("jour_semaine", "heure") \
        .agg(count("*").alias("nb_accidents")) \
        .orderBy(col("nb_accidents").desc())

    result.show()
    return result

def analyse_classement_departements(dfs):
    """Window function : classement des départements par nombre d'accidents"""
    print("\n" + "="*60)
    print("ANALYSE 3 (WINDOW) - CLASSEMENT DES DÉPARTEMENTS")
    print("="*60)

    dep_counts = dfs["caract"].groupBy("dep").agg(count("*").alias("nb_accidents"))

    window_spec = Window.orderBy(col("nb_accidents").desc())
    result = dep_counts.withColumn("rang", dense_rank().over(window_spec)) \
        .orderBy("rang")

    result.show(20)
    return result

# ============================================================================
# OPTIMISATION (mesurée)
# ============================================================================

def mesure_optimisation_cache(dfs):
    """Mesure l'effet du cache sur 'caract', réutilisé plusieurs fois en aval"""
    print("\n" + "="*60)
    print("OPTIMISATION - CACHE SUR 'caract' (réutilisé plusieurs fois)")
    print("="*60)

    caract = dfs["caract"]

    def trois_actions(df):
        df.count()
        df.groupBy("dep").count().count()
        df.groupBy("lum").count().count()

    # Sans cache : chaque action redéclenche la lecture CSV + le nettoyage complets
    debut = time.time()
    trois_actions(caract)
    duree_sans_cache = time.time() - debut
    print(f"Sans cache : {duree_sans_cache:.2f} s pour 3 actions")

    # Avec cache : la première action matérialise le résultat, les suivantes le réutilisent
    caract_cache = caract.cache()
    debut = time.time()
    trois_actions(caract_cache)
    duree_avec_cache = time.time() - debut
    print(f"Avec cache : {duree_avec_cache:.2f} s pour 3 actions")

    gain = (1 - duree_avec_cache / duree_sans_cache) * 100
    print(f"Gain : {gain:.1f}%")

    caract_cache.unpersist()
    return duree_sans_cache, duree_avec_cache

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Initialisation
    spark = init_spark()
    
    # Chargement des données (Bronze layer)
    print("Chargement des données...")
    dfs = {
        "caract": load_csv(spark, "data/caract-2024.csv", SCHEMAS["caract"]),
        "lieux": load_csv(spark, "data/lieux-2024.csv", SCHEMAS["lieux"]),
        "vehicules": load_csv(spark, "data/vehicules-2024.csv", SCHEMAS["vehicules"]),
        "usagers": load_csv(spark, "data/usagers-2024.csv", SCHEMAS["usagers"])
    }
    
    # Affichage du schéma et premiers enregistrements
    print("\nSchéma - Caractéristiques:")
    dfs["caract"].printSchema()
    print("\nAperçu des données:")
    dfs["caract"].show(5)
    
    # Analyse
    analyze_data(dfs)
    
    # Nettoyage : doublons + lignes sans clé de jointure
    dfs = clean_data(dfs)

    # Nettoyage : valeurs aberrantes
    dfs = clean_outliers(dfs)

    # Vérification des valeurs manquantes sur les 4 tables
    for name, df in dfs.items():
        check_missing_values(df, name)

    # Écriture de la couche Silver, partitionnée par colonne à faible cardinalité
    # (à décommenter une fois winutils.exe / HADOOP_HOME configurés sur la machine)
    print("\n" + "="*60)
    print("EXPORT COUCHE SILVER")
    print("="*60)
    # dfs["caract"].write.mode("overwrite").partitionBy("dep").parquet("output/silver/caract")
    # dfs["lieux"].write.mode("overwrite").partitionBy("catr").parquet("output/silver/lieux")
    # dfs["vehicules"].write.mode("overwrite").partitionBy("catv").parquet("output/silver/vehicules")
    # dfs["usagers"].write.mode("overwrite").partitionBy("catu").parquet("output/silver/usagers")
    print("✓ Prêt pour export (à décommenter une fois winutils configuré)")

    # Optimisation mesurée : cache sur 'caract' (réutilisé par les 3 analyses)
    mesure_optimisation_cache(dfs)

    # Analyses (Gold layer) : agrégation, jointure, window function
    analyse_gravite_par_meteo(dfs)
    analyse_accidents_par_heure_jour(dfs)
    analyse_classement_departements(dfs)

    print("\n✓ Pipeline exécuté avec succès!")

    print("\n" + "="*60)
    print(f"Spark UI disponible sur : {spark.sparkContext.uiWebUrl}")
    print("Va l'ouvrir dans ton navigateur pour lire le DAG et les shuffles.")
    input("Appuie sur Entrée ici quand tu as fini de regarder la Spark UI...")

    spark.stop()