Rapport de projet - Pipeline Spark (Jour 4)

 dev

Équipe : NWAGOUM WAMENE HARROLD




Équipe : [noms]

Jeu de données : ONISR (accidents corporels de la circulation routière, France, 2024)
Date : 2026-07-05

## 1. Jeu de données et schéma cible
 dev
Source : [data.gouv.fr — Bases de données annuelles des accidents corporels de la circulation routière (2005-2024)](https://www.data.gouv.fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024), millésime 2024, téléchargé le 2026-07-05.

Volume : 4 fichiers CSV reliés par `Num_Acc` (séparateur `;`, encodage UTF-8, décimales GPS en virgule) :

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

Preuve du diff (colonnes attendues par le squelette de départ vs colonnes réelles de l'en-tête CSV 2024) :
- `lieux` — squelette de départ : `Num_Lieu, nom, adr, lat, long, dep, com, agg, int, atm, col, hrmn, lum, jour, mois, an` (16 colonnes, en fait un doublon des colonnes de `caract`) ↔ réel : `Num_Acc, catr, voie, v1, v2, circ, nbv, vosp, prof, pr, pr1, plan, lartpc, larrout, surf, infra, situ, vma` (18 colonnes, aucune en commun sauf `Num_Acc` absent du squelette).
- `usagers` — squelette de départ : `Num_Usager, Num_Veh, Num_Acc, Num_Lieu, nom, dep, com, agg, int, atm, col, hrmn, lum, jour, mois, an, lat, long, adr` (19 colonnes, même confusion avec `caract`) ↔ réel : `Num_Acc, id_usager, id_vehicule, num_veh, place, catu, grav, sexe, an_nais, trajet, secu1, secu2, secu3, locp, actp, etatp` (16 colonnes).

Encodage vérifié (pas juste supposé) : recherche d'octets non-ASCII dans `caract-2024.csv` (`grep -P "[\xC0-\xFF]"`) — les caractères accentués (« Allée », « Félix », « Alliés ») apparaissent comme des séquences UTF-8 valides à 2 octets (ex. `é` = `0xC3 0xA9`), confirmé par ailleurs par `file` (`UTF-8 text` sur les 4 CSV). Pas de mauvaise lecture en `latin1`.

Statistiques `describe()` sur les colonnes numériques sensibles (avant filtrage des valeurs aberrantes), utilisées pour justifier les bornes de nettoyage plutôt que les poser a priori :
```
lat/long (caract, 54 402 lignes, aucun null après cast) :
  lat  : min -22.43   max  51.08   mean  44.05   stddev 12.67
  long : min -178.09  max 167.86   mean   1.24   stddev 19.46

an_nais (usagers, 122 608 valeurs non nulles sur 125 187) :
  min 1914   max 2024   mean 1985.08   stddev 19.33

vma (lieux, 66 618 valeurs non nulles sur 70 248) :
  min 0   max 900   mean 56.69   stddev 24.71
```
Lecture : `lat`/`long` et `an_nais` ne contiennent en réalité aucune valeur hors bornes plausibles pour ce millésime (les bornes servent de garde-fou, pas de correction active) — la borne `lat` a d'ailleurs été resserrée par erreur à `51` puis corrigée à `51.1` en comparant au max réel observé (51.08, cohérent avec le point le plus au nord de la France métropolitaine). `vma`, en revanche, contient une vraie valeur aberrante (max 900 km/h), confirmant que la borne `[0, 300]` est nécessaire et efficace.



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

 dev
Écriture effective de la couche silver : `dfs[...].write.mode("overwrite").partitionBy(...).parquet("output/silver/...")` pour les 4 tables. Bloquée initialement sous Windows natif (`winutils.exe`/`HADOOP_HOME` manquants pour l'écriture locale via Hadoop) ; débloquée en exécutant le pipeline sous Debian (WSL2), où Spark écrit directement sur un filesystem Linux sans dépendance à Hadoop natif Windows. Vérifié : `output/silver/caract/` contient bien un fichier `_SUCCESS` et un sous-dossier par valeur de `dep` (`dep=01`, `dep=02`, ...), même schéma de partitionnement pour les 3 autres tables.


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

 dev
Mesure avant/après (3 actions : `count()`, `groupBy("dep").count()`, `groupBy("lum").count()`, exécutées deux fois de suite pour vérifier la reproductibilité — code : `chronometre()` dans `mesure_optimisation_cache`, [pipeline.py](../pipeline.py#L260-L299)) :

```
Sans cache : 3.34 s puis 1.37 s
Avec cache : 6.65 s puis 4.38 s
Gain (passage 1) : -99.1 %
Gain (passage 2) : -219.1 % (le cache est 3 à 4x plus LENT ici)
```

**Résultat contre-intuitif, expliqué avec extrait de plan réel (`.explain()` exécuté dans le script, pas recopié de mémoire)** : `caract` ne fait que 5 partitions et ~54 000 lignes issues d'un CSV de quelques Mo — la lecture + le nettoyage sont déjà bon marché. Point de méthode : le premier `.explain()` doit être capturé **avant** l'appel à `.cache()`, sinon `caract` et `caract_cache` partagent le même plan logique déjà mis en cache et les deux extraits sont identiques (piège rencontré et corrigé pendant la rédaction de ce rapport).




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

 dev




Deux `Exchange` (shuffles) apparaissent, pas un seul :
1. **Shuffle n°1** : provient de `dropDuplicates()` dans `clean_data`. Spark l'implémente comme un `groupBy` sur *toutes* les colonnes (15 pour `caract`) pour détecter les doublons — un shuffle sur l'ensemble des colonnes, donc plus coûteux qu'il n'y paraît dans le code (`dropDuplicates()` a l'air anodin mais déclenche un repartitionnement complet des données).
2. **Shuffle n°2** : le vrai `groupBy("dep")` de l'analyse — repartitionnement sur la seule colonne `dep` pour regrouper les lignes du même département avant de compter.

Le même schéma (shuffle du `dropDuplicates` + shuffle du `groupBy`/`join` de l'analyse) se retrouve dans les 3 analyses et dans l'optimisation cache, puisqu'elles partagent toutes la même lignée `caract` nettoyée.
 dev
Nombre de stages et de tasks — **vérifié via l'API REST de la Spark UI** (`/api/v1/applications/<id>/stages`), pas juste lu à l'œil sur l'UI : job isolé sur `caract.groupBy("dep").count().count()`, 3 stages, **4 tasks au total (2 puis 1 puis 1)** — très loin des « jusqu'à 200 tasks » qu'on pourrait attendre de `spark.sql.shuffle.partitions`.

Explication : Spark 4 active l'**Adaptive Query Execution (AQE)** par défaut (visible dans le plan via `AdaptiveSparkPlan`), dont l'optimisation `CoalescePartitions` fusionne automatiquement les partitions de shuffle trop petites après avoir mesuré la taille réelle des données au runtime. Sur ce volume (54 402 lignes, quelques dizaines de Ko par partition de shuffle), AQE ramène les 200 partitions théoriques à 1 ou 2 partitions effectives — la valeur `200` de `spark.sql.shuffle.partitions` n'est donc qu'un plafond de configuration, pas le nombre de tasks réellement exécutées. C'est directement la piste d'exploration « AQE et nombre de partitions » proposée en section 6.

Capture(s) : DAG réel du job (Spark UI, onglet **Jobs** → job sur `caract.groupBy("dep").count()`, port 4040) :

![DAG du job groupBy("dep").count() — Stage 270 (cache, skipped) puis Stages 271-273](screenshots/Dag-134.png)

On y voit concrètement la lecture du cache (`InMemoryTableScan`, bloc gris "Stage 270 (skipped)") puis l'`Exchange` du `groupBy("dep")` (Stage 271) suivi de deux stages de réduction (272, 273) — la traduction visuelle des deux shuffles décrits dans le plan `.explain()` ci-dessus.

Commentaire : deux shuffles pour une opération qui semble être « juste un `groupBy` » dans le code est le premier enseignement de cette lecture de plan (le `dropDuplicates()` du nettoyage n'est pas gratuit et se répercute sur les 3 analyses) ; le second, plus surprenant, est que le nombre de tasks réellement exécuté ne suit pas la configuration statique `spark.sql.shuffle.partitions` dès que l'AQE est active — lire un plan ne suffit pas, il faut aussi vérifier les métriques d'exécution réelles pour ne pas se fier à une valeur de configuration qui ne reflète plus le comportement réel du moteur.

*Note méthodologique* : le plan `.explain()`, les métriques de stages et la capture ci-dessus proviennent de requêtes réelles exécutées et interrogées pendant la session de travail (API REST Spark UI), pas d'une supposition.

## 6. Exploration au-delà du cours

Piste choisie : **AQE et nombre de partitions**. Elle découle directement d'une anomalie repérée en section 5 : le rapport supposait `spark.sql.shuffle.partitions` (200 par défaut) directement responsable du nombre de tasks exécutées, alors que l'API REST de la Spark UI montrait un nombre de tasks bien plus faible en pratique. Cette exploration mesure pourquoi.

**Question** : sur ce volume (54 402 lignes), quel est l'effet réel de l'Adaptive Query Execution (AQE) et du nombre de partitions de shuffle sur (a) le nombre de tasks effectivement exécutées et (b) le temps d'exécution d'une agrégation (`caract.groupBy("dep").count()`) ?

**Protocole** (code : `exploration_aqe_partitions()`, [pipeline.py](../pipeline.py#L307-L363)) : un seul réglage varie à la fois, tout le reste est fixe.
- Fixe : la requête (`caract.groupBy("dep").count().count()`), `caract` mis en cache une fois avant les 3 mesures pour isoler l'effet du shuffle du `groupBy` (ne pas reconfondre avec le coût de relecture CSV + `dropDuplicates`), même machine, même session Spark.
- Varie : `spark.sql.adaptive.enabled` (on/off) et `spark.sql.shuffle.partitions` (200 par défaut vs 8 réglé manuellement), changés à chaud via `spark.conf.set(...)` entre chaque mesure.
- Mesure : temps (`time.time()`) et nombre de tasks par stage du job, lu via l'API REST de la Spark UI (`/api/v1/applications/<id>/jobs` puis `/stages`) — pas une lecture visuelle de l'onglet Stages.

**Mesures** :
```
AQE on (défaut Spark 4), 200 partitions                  : 2.81 s, tasks par stage = [2, 200, 1, 1]
AQE off, 200 partitions (config par défaut sans AQE)     : 2.89 s, tasks par stage = [2, 200, 200, 1]
AQE off, 8 partitions (réglage manuel pour ce volume)    : 1.10 s, tasks par stage = [2, 200, 8, 1]
```
Le stage de réduction du `groupBy("dep")` (3ᵉ valeur de la liste) passe de 200 tasks (AQE off, réglage par défaut) à 1 task (AQE on) — l'`AdaptiveSparkPlan` recalcule la taille réelle des données après le shuffle et coalesce les partitions trop petites, confirmant l'hypothèse de la section 5. La 2ᵉ valeur (200) reste constante dans les 3 mesures : c'est un stage de shuffle-map déjà matérialisé lors de la mise en cache initiale de `caract` (avant la boucle de test), listé comme "skipped" par l'API pour les mesures suivantes plutôt que ré-exécuté — un artefact de méthode à noter plutôt qu'un résultat en soi.

Capture (Spark UI, onglet **Stages** → "Stages for All Jobs") montrant les deux cascades côte à côte, avec les octets de shuffle réels :

![Tableau des stages : cascade 200→200→1 (AQE off, 200 partitions, stages 267-269) et 200→8→1 (AQE off, 8 partitions, stages 271-273)](screenshots/Tableau%20de%20stage.png)

Lecture directe du tableau : stages 267-269 (`200/200` puis `200/200` puis `1/1`, shuffle write 885,7 KiB puis 11,2 KiB) correspondent à la config "200 partitions par défaut" — le stage intermédiaire ne se réduit pas. Stages 271-273 (`200/200` puis `8/8` puis `1/1`, shuffle write 228,7 KiB puis 472 B) correspondent à la config "8 partitions manuelles" — la réduction est immédiate dès le 2ᵉ stage, avec beaucoup moins d'octets à mélanger.

**Conclusion (contre-intuitive)** : l'AQE fait bien son travail (200 → 1 task sur le stage final), mais **le réglage manuel à 8 partitions est ~2,6x plus rapide que l'AQE** (1,10 s contre 2,81 s), et même légèrement plus rapide que la config par défaut sans AQE. L'AQE ajoute un coût de replanification adaptative (collecte de statistiques après le premier stage de shuffle, re-optimisation du plan) qui, sur un volume aussi petit, pèse plus lourd que le gain apporté par le coalescing. Pour ce pipeline (volume fixe et connu à l'avance), fixer `spark.sql.shuffle.partitions` à une valeur adaptée au volume réel bat à la fois l'AQE et le réglage par défaut. L'AQE reste utile comme filet de sécurité générique quand le volume varie ou est inconnu à l'avance (cas le plus courant en production) — mais ce n'est pas un remplacement automatique d'un réglage manuel pertinent sur un pipeline au volume stable.

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
 dev
- Écriture de la couche silver en Parquet, débloquée en migrant l'exécution du pipeline vers Debian (WSL2) : contournement du blocage `winutils.exe`/`HADOOP_HOME` propre à Spark sous Windows natif, sans changer une ligne de code métier.

Ce qui a bloqué :
- Spark sous Windows natif ne peut pas écrire localement sans `winutils.exe`/`HADOOP_HOME` — contourné en exécutant le pipeline dans Debian (WSL2), où l'écriture Parquet fonctionne nativement.
- Antivirus (Norton 360) mettant en quarantaine des fichiers nécessaires à PySpark lors de l'installation sous Windows (`spark-submit.cmd`, `pyspark.cmd`, `java_gateway.py`), provoquant une installation incomplète. Résolu en ajoutant des exclusions Norton (dans les sections "Exclusions des analyses" et "Exclusions de la surveillance automatique") pour les dossiers `.venv`, Java 17 et `AppData/Local/Temp`, puis en restaurant les fichiers mis en quarantaine — Spark s'exécute ensuite sans interruption.


Ce qui a bloqué :
- Écriture de la couche silver en Parquet non encore effective sur cette machine (Windows, `winutils.exe`/`HADOOP_HOME` manquants) — le nettoyage est validé en mémoire mais pas encore persisté.

- Le cache, optimisation "évidente" sur le papier, s'est révélé contre-productif sur ce volume — bon rappel qu'une optimisation doit toujours être mesurée, jamais supposée.

Ce qu'on ferait avec plus de temps :
- Retester le cache sur un volume multi-années pour identifier le seuil à partir duquel il devient rentable.
dev
- Lire la couche silver Parquet fraîchement écrite pour vérifier le partition pruning (plan `.explain()` avec filtre sur la colonne de partition).

- Écrire réellement la couche silver partitionnée une fois l'environnement Hadoop/Windows réglé.

