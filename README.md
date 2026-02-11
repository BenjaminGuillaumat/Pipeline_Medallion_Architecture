# Data Pipeline Engineering - Medallion Architecture

#### Auteur :  **Benjamin Guillaumat-Tailliet**

Ce projet implémente une architecture **Medallion (Bronze, Silver, Gold)** pour traiter et analyser des données liées aux établissements d'enseignement supérieur et aux résultats du baccalauréat en France.  

L'objectif est de fournir des indicateurs de performance académique stockés en base de données relationnelle pour une exploitation BI.


## Architecture & Stack Technique

- **Moteur de calcul** : Apache Spark (Scala)  
- **Orchestration & Format** : Spark-submit & Parquet  
- **Base de données** : PostgreSQL (Dockerisé)  
- **Gestion de dépendances** : Maven  
- **Tests** : ScalaTest  


## Pipeline de Données

Le workflow est divisé en trois zones de données pour garantir la qualité et la traçabilité :

### Zone Bronze (Ingestion)

- Intégration des datasets sources au format CSV (données d'implantation, contours géographiques, résultats du bac).  
- Stockage brut pour permettre le retraitement si nécessaire.  


### Zone Silver (Transformation & Nettoyage)

Cette étape assure l'intégrité et l'homogénéité des données :

- **Normalisation Schema-on-read** : Standardisation des noms de colonnes (minuscules, suppression des accents et caractères spéciaux).  
- **Data Cleaning** : Harmonisation des (code_academie) et des libellés de sexe (mapping des valeurs hétérogènes vers un standard unique).  
- **Enrichissement** : Ajout de colonnes techniques ((insertion_date)) et jointures géographiques pour ajouter les libellés de régions.  
- **Optimisation** : Persistance des données au format Parquet.  

### Zone Gold (Analytics & Business)

Calcul des KPIs métier et export vers PostgreSQL :

- **Répartition géographique** : Nombre d’établissements supérieurs par type et par région.  
- **Indicateurs de réussite** : Taux de réussite par académie et par genre.  
- **Comparaison de performance** : Benchmark national vs local.  


## Exécution


#### 1. Démarrer l'infrastructure : ```docker compose up -d```

#### 2. Compiler le projet : ```mvn clean package```

#### 3. Soumettre le Job Spark : ```spark-submit --class com.polytech.MainApp --master "local[*]" --packages org.postgresql:postgresql:42.6.0 ./target/tp_spark-job-1.0.0-SNAPSHOT.jar```

## Tests

Le projet suit une approche de test pour chaque étape de transformation :

- **Tests Unitaires (Silver)** : Validation des fonctions de normalisation de colonnes, des formats de codes académiques et de la logique de nettoyage des strings.  
- **Tests d'Intégration (Gold)** : Vérification de l'exactitude des agrégations ```(groupBy)``` et des calculs de taux de réussite avant l'insertion en base.  

#### Exécution des tests : ```mvn test```
