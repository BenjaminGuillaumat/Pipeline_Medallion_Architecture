# TP BIG DATA - Benjamin Guillaumat-Tailliet

##### LANCEMENT DE LA PIPELINE #####

### Démarrer Docker Postgres
docker compose up -d

### Build l'application avec Maven
mvn clean package

### Lancer la pipeline SILVER et GOLD
spark-submit \
  --class com.polytech.MainApp \
  --master "local[*]" \
  --packages org.postgresql:postgresql:42.6.0 \
  ./target/tp_spark-job-1.0.0-SNAPSHOT.jar


##### EXPLICATIONS TP #####

### Zone Bronze
La zone Bronze concerne l'intégration des données au format csv. Ici, nous travaillons sur 3 datasets au format csv disponibles dans le dossier "src/main/resources/data/bronze".

### Zone Silver
La zone Silver concerne le traitement des données, nettoyage et enrichissement des données. Voici les étapes :

- 1) Lectures des données aux format csv et création des dataframes correspondants.
- 2) Normalisation des noms des colonnes des 3 dataframes. Mise en minuscule, suppression des accents, suppression des tirets...
- 3) Normalisation du code_academie dans dfn_implantations (A07 -> 7 / A33 -> 33) pour cohérence avec les 2 autres colonnes code_academie des 2 autres df.
- 4) Normalisation des valeurs de la colonne "sexe" dans dfn_bac pour se limiter à "FEMININ" et "MASCULIN". Auparavent on retrouvait ces valeurs : "filles", "garçons", "féminin", "masculin".
- 5) Ajout column "insertion_date" avec la date courante pour les 3 dataframes.
- 6) Ajout nom libelle region dans dfn_bac via jointure avec dfn_contour.
- 7) Enfin, écriture des DataFrames en format Parquet pour prochaine zone (gold).

### Zone Gold
La zone gold concerne le requêtage des données et l'insertion des résultats dans une base de données postgresql. voici les étapes :

- 1) Lecture des DataFrames Parquet depuis le répertoire Silver
- 2) Partie 1 : nombre d’établissement supérieur, par type d’établissement, par région. Pour cela nous faisons une jointure entre les df "dfn_implantations" et "dfn_contour".
- 3) Partie 2 : Cette partie comporte 3 questions qui font aboutissent sur 3 résultats.
  - Partie 2.1 : Pourcentage de réussite masculin et feminin compris pour l’académie en question la même année
  - Partie 2.2 : Pourcentage de réussite national sur l’année pour le même genre
  - Partie 2.3 : Pourcentage de réussite national sur l’année, quelque soit le genre
- 4) Enregistrement des résultats dans PostgreSQL (avec instance postgres docker)
 
### Tests unitaires
Les tests unitaires ont été réalisé sur la partie silver et gold. Ils sont écrits dans le dossier "src/test/scala" dans les fichiers respectifs "silver_Test.scala" et "gold_Test.scala".

Voici les tests réalisés dans chacun de ces fichiers :
1) silver_Test.scala :
  - Test de la fonction normalizeColumns pour normaliser les chaînes de caractères
  - Test normalisation des codes académie dans dfn_implantations
  - Test normalisation des valeurs de la colonne sexe dans dfn_bac
  - Test d'ajout de la colonne insertion_date

2) gold_Test.scala :
  - Test du groupBy et count pour le nombre d’établissement supérieur par type d’établissement par région
  - Test du calcul du taux de réussite par académie et par genre
