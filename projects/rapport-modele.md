Rapport de projet - Pipeline Spark (Jour 4)

Gabarit du livrable noté. Remplir chaque section. Court et dense : extraits de code, extraits de résultats, captures. Pas de pavé. Les sections reprennent le plan du rapport (section 5 de projects/projet-jour-4.md). La grille reste le barème ; la qualité du code est notée sur le code lui-même, pas dans ce document.

Équipe : [noms]
Jeu de données : ONISR (accidents corporels de la circulation routière, France, 2024)
Date : 2026-07-05

## 1. Jeu de données et schéma cible

Source et volume : ONISR, 4 fichiers CSV reliés par `Num_Acc` (séparateur `;`, encodage UTF-8, décimales GPS en virgule) :
- `caract-2024.csv` — 54 402 lignes — caractéristiques de l'accident (date, heure, lieu, météo, luminosité, collision)
- `lieux-2024.csv` — 70 248 lignes — description du lieu (catégorie de route, profil, vitesse max autorisée...)
- `vehicules-2024.csv` — 92 678 lignes — véhicules impliqués (catégorie, choc, manœuvre...)
- `usagers-2024.csv` — 125 187 lignes — usagers impliqués (gravité, âge, sexe, équipement de sécurité...)

Schéma cible (colonnes retenues, types) :
- `caract` : `Num_Acc (long)`, `jour/mois/an (int)`, `hrmn (string)`, `lum/agg/int/atm/col (int)`, `dep/com (string)`, `adr (string)`, `lat/long (string, virgule décimale → castées en double au nettoyage)`
- `lieux` : `Num_Acc (long)`, `catr/circ/nbv/vosp/prof/plan/surf/infra/situ/vma (int)`, `voie/v1/v2/pr/pr1/lartpc/larrout (string)`
- `vehicules` : `Num_Acc (long)`, `id_vehicule/num_veh (string)`, `senc/catv/obs/obsm/choc/manv/motor/occutc (int)`
- `usagers` : `Num_Acc (long)`, `id_usager/id_vehicule/num_veh/actp (string)`, `place/catu/grav/sexe/an_nais/trajet/secu1/secu2/secu3/locp/etatp (int)`

**Point d'attention découvert en cours de projet** : le schéma explicite du squelette de départ pour `lieux` et `usagers` ne correspondait pas du tout aux vraies colonnes des fichiers 2024 (noms et nombre de colonnes différents — probablement un gabarit d'une autre année du jeu de données). Un contrôle `printSchema()` + comparaison avec l'en-tête réel du CSV a permis de le détecter et de corriger les deux schémas avant toute analyse. Sans ce contrôle, toutes les analyses en aval auraient été construites sur des colonnes mal alignées.

Questions métier visées : la gravité varie-t-elle selon la météo ? Quels créneaux horaires concentrent le plus d'accidents ? Quels départements sont les plus touchés ?

## 2. Pipeline (bronze -> silver -> gold)

```
brut (bronze)  ->  nettoyé (silver, Parquet)  ->  agrégé (gold)
```

**Bronze** : lecture des 4 CSV avec schéma explicite (`spark.read.schema(...).csv(...)`), sans `inferSchema`.

Nettoyage appliqué (filtres, manquants, doublons) :
- Doublons : `dropDuplicates()` sur les 4 tables → 0 doublon détecté sur ce millésime.
- Lignes sans clé de jointure : filtre `Num_Acc IS NOT NULL` → 0 ligne écartée.
- Valeurs aberrantes (mises à `null`, sans suppression de ligne, pour ne pas perdre le reste de l'enregistrement) :
  - `caract.lat` / `caract.long` : conversion virgule → point puis cast `double` ; hors des bornes plausibles pour la France (métropole + DOM-TOM, `lat ∈ [-22, 51]`, `long ∈ [-178, 168]`) → `null`.
  - `usagers.an_nais` : hors `[1900, 2024]` → `null`.
  - `lieux.vma` : hors `[0, 300]` km/h → `null`.
- Valeurs manquantes relevées (`check_missing_values` sur les 4 tables), exemples marquants :
  - `caract` : `long` (1), `lat` (140 après filtrage hors-bornes), `col` (6), `adr` (2).
  - `lieux` : `lartpc` (70 215 / 70 248 — champ presque toujours vide), `vma` (3 656), `circ` (4 354).
  - `vehicules` : `occutc` (91 729 / 92 678 — normal, ne concerne que les transports en commun).
  - `usagers` : `etatp` (114 932), `secu3` (113 133) — champs qui ne s'appliquent qu'aux piétons/cas particuliers.

Lignes brutes : 54 402 (caract) / 70 248 (lieux) / 92 678 (vehicules) / 125 187 (usagers) | après nettoyage : identique (0 doublon, 0 ligne sans clé) | écartées : 0 %. Le nettoyage de ce millésime porte sur la mise à `null` de valeurs aberrantes ponctuelles, pas sur la suppression de lignes.

Partitionnement de la silver (colonne, pourquoi) : colonne catégorielle à faible cardinalité propre à chaque table, pour permettre le partition pruning sur les requêtes futures — `dep` (département) pour `caract`, `catr` (catégorie de route) pour `lieux`, `catv` (catégorie de véhicule) pour `vehicules`, `catu` (catégorie d'usager) pour `usagers`.

*(Écriture effective de la couche silver en attente : blocage d'environnement Windows — `winutils.exe`/`HADOOP_HOME` manquants pour l'écriture locale via Hadoop. Le nettoyage est validé en mémoire ; l'écriture sera débloquée avant la fin du projet.)*

## 3. Analyses

### Analyse 1 - agrégation

**Question** : quels créneaux horaires (jour de la semaine × heure) concentrent le plus d'accidents ?

**Code clé** :
```python
caract = caract \
    .withColumn("heure", split(col("hrmn"), ":").getItem(0).cast(IntegerType())) \
    .withColumn("date", to_date(concat_ws("-", col("an"), col("mois"), col("jour")))) \
    .withColumn("jour_semaine", date_format(col("date"), "E"))

result = caract.groupBy("jour_semaine", "heure") \
    .agg(count("*").alias("nb_accidents")) \
    .orderBy(col("nb_accidents").desc())
```

**Résultat (extrait, top 5)** :
```
+------------+-----+------------+
|jour_semaine|heure|nb_accidents|
+------------+-----+------------+
|         Fri|   17|         737|
|         Thu|   17|         736|
|         Fri|   18|         730|
|         Tue|   17|         719|
|         Mon|   17|         715|
+------------+-----+------------+
```

**Lecture métier** : pic net le vendredi et le jeudi entre 17h et 18h, les autres jours ouvrés suivant la même tendance à 17h-18h, avec un second pic secondaire vers 8h en semaine. Signature typique des trajets domicile-travail : la pointe du soir dépasse systématiquement celle du matin.

### Analyse 2 - jointure

**Question** : les conditions météo dégradées aggravent-elles la gravité des accidents ?

**Code clé** :
```python
result = usagers.select("Num_Acc", "grav") \
    .join(caract.select("Num_Acc", "atm"), on="Num_Acc", how="inner") \
    .groupBy("atm") \
    .agg(
        count("*").alias("nb_usagers"),
        round(avg(when(col("grav") == 2, 1).otherwise(0)) * 100, 2).alias("pct_tues")
    ) \
    .orderBy(col("pct_tues").desc())
```

**Résultat (extrait)** :
```
+---+----------+--------+
|atm|nb_usagers|pct_tues|
+---+----------+--------+
|  6|       364|    7.14|
|  5|      1306|    6.51|
|  9|       529|    4.16|
|  3|      3417|    3.34|
|  7|      1824|    3.34|
|  1|     96243|    2.73|
|  4|       613|    2.61|
|  8|      5400|     2.5|
|  2|     15491|    2.23|
+---+----------+--------+
```

**Lecture métier** : le taux de tués est le plus élevé par temps éblouissant (`atm=6`, 7,14 %) et par brouillard (`atm=5`, 6,51 %) — des conditions rares (364 et 1 306 usagers) mais nettement plus dangereuses. Le temps « normal » (`atm=1`) concentre l'écrasante majorité des usagers (96 243) avec un taux de gravité modéré (2,73 %), et le temps couvert (`atm=2`) affiche le taux le plus bas (2,23 %). Le volume d'accidents est dominé par la conduite en conditions normales, mais le risque individuel par usager grimpe fortement dans les conditions météo rares et dégradées.

### Analyse 3 - window function

**Question** : quels départements concentrent le plus d'accidents ?

**Code clé** :
```python
dep_counts = caract.groupBy("dep").agg(count("*").alias("nb_accidents"))

window_spec = Window.orderBy(col("nb_accidents").desc())
result = dep_counts.withColumn("rang", dense_rank().over(window_spec)) \
    .orderBy("rang")
```

**Résultat (extrait, top 5)** :
```
+---+------------+----+
|dep|nb_accidents|rang|
+---+------------+----+
| 75|        4191|   1|
| 93|        2640|   2|
| 92|        2485|   3|
| 13|        2120|   4|
| 94|        1963|   5|
+---+------------+----+
```

**Lecture métier** : Paris (75) domine très largement avec 4 191 accidents, suivi par la petite couronne (Seine-Saint-Denis 93, Hauts-de-Seine 92, Val-de-Marne 94) puis les Bouches-du-Rhône (13). Le classement suit clairement la densité de population et de circulation urbaine plutôt que la superficie des départements.

## 4. Optimisation

Optimisation choisie : cache — sur `caract` (nettoyé), réutilisé par les 3 analyses ci-dessus plus la vérification des valeurs manquantes.

Pourquoi : sans cache, Spark étant paresseux, chaque action (`.show()`, `.count()`) redéclenche toute la chaîne de lecture CSV + nettoyage (dont `dropDuplicates`, une opération avec shuffle) depuis le disque.

Mesure avant/après (3 actions : `count()`, `groupBy("dep").count()`, `groupBy("lum").count()`, exécutées deux fois de suite pour vérifier la reproductibilité) :

```
avant (sans cache) : 2.89 s puis 3.16 s
après (avec .cache()) : 17.59 s puis 18.83 s
Gain : -508 % / -496 % (le cache est ~6x plus LENT ici)
```

**Résultat contre-intuitif, expliqué avec extrait de plan** : `caract` ne fait que 5 partitions et ~54 000 lignes issues d'un CSV de quelques Mo — la lecture + le nettoyage sont déjà bon marché. `explain()` montre que la version cache remplace le `FileScan` par un `InMemoryTableScan` sur une `InMemoryRelation` en `StorageLevel(disk, memory, deserialized)` :

```
--- sans cache ---
HashAggregate(keys=[dep#6], ...)
+- Exchange hashpartitioning(dep#6, 200), ...
   +- HashAggregate(...)
      +- FileScan csv [...] Format: CSV, ...

--- avec cache ---
HashAggregate(keys=[dep#6], ...)
+- Exchange hashpartitioning(dep#6, 200), ...
   +- InMemoryTableScan [dep#6]
         +- InMemoryRelation [...], StorageLevel(disk, memory, deserialized, 1 replicas)
```

Le coût de matérialisation en cache (sérialisation + gestion mémoire du storage level `MEMORY_AND_DISK`) dépasse ici le gain de ne pas relire un petit fichier CSV, dans un Spark local mono-JVM (driver = executor, ressources partagées avec le reste du script). Conclusion : sur un petit volume avec peu de réutilisations, le cache n'est pas automatiquement gagnant — son intérêt croît avec la taille des données et/ou le nombre de réutilisations (à re-tester sur un jeu multi-années pour confirmer le seuil de rentabilité).

Ce que ça change : décision de ne pas garder le cache sur `caract` pour ce volume de données ; piste à documenter comme résultat négatif plutôt que comme échec.

## 5. Lecture de la Spark UI

Job observé : n'importe quelle action déclenchée sur `caract` après nettoyage (ex. `caract.groupBy("dep").count()`, sous-jacent à l'Analyse 3) — `caract` compte 5 partitions en entrée (issues du split du CSV), confirmé par `caract.rdd.getNumPartitions()`.

Où se produit le shuffle (Exchange) — extrait réel de `.explain()` :
```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[dep#6], functions=[count(1)])
   +- Exchange hashpartitioning(dep#6, 200), ENSURE_REQUIREMENTS         <-- shuffle n°2
      +- HashAggregate(keys=[dep#6], functions=[partial_count(1)])
         +- HashAggregate(keys=[15 colonnes de caract], functions=[])
            +- Exchange hashpartitioning(15 colonnes, 200), ENSURE_REQUIREMENTS  <-- shuffle n°1
               +- HashAggregate(keys=[15 colonnes], functions=[])
                  +- Filter isnotnull(Num_Acc#0L)
                     +- FileScan csv [...]
```

Deux `Exchange` (shuffles) apparaissent, pas un seul :
1. **Shuffle n°1** : provient de `dropDuplicates()` dans `clean_data`. Spark l'implémente comme un `groupBy` sur *toutes* les colonnes (15 pour `caract`) pour détecter les doublons — un shuffle sur l'ensemble des colonnes, donc plus coûteux qu'il n'y paraît dans le code (`dropDuplicates()` a l'air anodin mais déclenche un repartitionnement complet des données).
2. **Shuffle n°2** : le vrai `groupBy("dep")` de l'analyse — repartitionnement sur la seule colonne `dep` pour regrouper les lignes du même département avant de compter.

Le même schéma (shuffle du `dropDuplicates` + shuffle du `groupBy`/`join` de l'analyse) se retrouve dans les 3 analyses et dans l'optimisation cache, puisqu'elles partagent toutes la même lignée `caract` nettoyée.

Nombre de stages et de tasks : 3 stages, séparés par les 2 `Exchange` ci-dessus.
- Stage 1 (lecture + filtre + agrégat partiel du dédoublonnage) : **5 tasks**, une par partition du fichier CSV source.
- Stage 2 (fin du dédoublonnage après le 1ᵉʳ shuffle + agrégat partiel du `groupBy`) : jusqu'à **200 tasks** — `spark.sql.shuffle.partitions` vaut 200 par défaut, alors que le volume réel (54 402 lignes / 200 ≈ 272 lignes par partition) ne le justifie pas. C'est un déséquilibre nombre-de-partitions/volume typique d'un petit jeu de données, à mettre en lien avec la piste d'exploration « AQE et nombre de partitions » (section 6).
- Stage 3 (agrégat final après le 2ᵉ shuffle, un par département) : jusqu'à 200 tasks également, mais très peu de données par task vu qu'il n'y a qu'une centaine de départements distincts au final.

Capture(s) : [à insérer — voir note ci-dessous]

Commentaire : deux shuffles pour une opération qui semble être « juste un `groupBy` » dans le code est le point le plus instructif de cette lecture de plan : le `dropDuplicates()` du nettoyage est loin d'être gratuit, et se répercute sur chacune des 3 analyses qui réutilisent `caract`.

*Note méthodologique* : ces informations proviennent d'un `.explain()` exécuté pendant la session de travail (plan physique réel, pas une supposition). La capture d'écran de la Spark UI (onglets **Jobs** → DAG → **Stages**, port 4040) reste à ajouter : elle nécessite d'ouvrir le navigateur pendant que le script tourne (voir les instructions données en cours de projet — lancer `pipeline.py` dans un terminal, `input()` maintient la session ouverte). À faire de préférence après avoir libéré de la RAM sur la machine (2,4 Go seulement disponibles au moment de la rédaction), une session Spark locale ayant besoin de marge mémoire pour démarrer la JVM sans erreur.

## 6. Exploration au-delà du cours

Piste choisie : [AQE et partitions / skew et salting / UDF vs pandas_udf / table gérée et upsert / spark-submit / pushdown mesuré / benchmark formats / streaming ou MLlib]
Question : [...]
Protocole (ce qu'on a fait varier, ce qui reste fixe) : [...]
Mesures : [...]
Conclusion (même si négative ou contre-intuitive) : [...]

*(à compléter)*

## 7. Ce qu'on a appris et limites

Ce qui a marché :
- La validation systématique du schéma explicite contre l'en-tête réel du CSV, faite tôt, a évité de construire toute l'analyse sur des colonnes mal alignées (voir section 1).
- Les trois analyses (agrégation, jointure, window) donnent des résultats cohérents et interprétables sans retraitement supplémentaire.

Ce qui a bloqué :
- Écriture de la couche silver en Parquet non encore effective sur cette machine (Windows, `winutils.exe`/`HADOOP_HOME` manquants) — le nettoyage est validé en mémoire mais pas encore persisté.
- Le cache, optimisation "évidente" sur le papier, s'est révélé contre-productif sur ce volume — bon rappel qu'une optimisation doit toujours être mesurée, jamais supposée.

Ce qu'on ferait avec plus de temps :
- Retester le cache sur un volume multi-années pour identifier le seuil à partir duquel il devient rentable.
- Écrire réellement la couche silver partitionnée une fois l'environnement Hadoop/Windows réglé.
