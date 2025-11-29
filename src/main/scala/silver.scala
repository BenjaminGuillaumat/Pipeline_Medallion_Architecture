package com.polytech

import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.Normalizer
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions._

object Silver {

    def normalize(str: String): String = {
      // Suppression des accents et normalisation des chaînes de caractères
      val noAccents = Normalizer.normalize(str, Normalizer.Form.NFD)
        .replaceAll("\\p{M}", "")

      noAccents
        .toLowerCase()          
        .replaceAll("'", "")
        .replaceAll("[^a-z0-9]+", "_")
        .replaceAll("_+$", "")
        .replaceAll("^_+", "")
    }

    def normalizeColumns(df: DataFrame): DataFrame = {
      val newCols = df.columns.map(c => normalize(c))
      df.toDF(newCols: _*)
    }

    def main(args: Array[String]): Unit = {

      // Initialisation de la session Spark
      val spark = SparkSession.builder()
        .appName("SilverApp")
        .master("local[*]")
        .getOrCreate()
      
      spark.sparkContext.setLogLevel("WARN")

      val sc = spark.sparkContext

      // Ajout des fichiers CSV aux fichiers distribués

      sc.addFile(
        """src/main/resources/data/bronze/fr-en-baccalaureat-par-academie.csv"""
      )
      sc.addFile(
        """src/main/resources/data/bronze/fr-en-contour-academies-2020.csv"""
      )
      sc.addFile(
        """src/main/resources/data/bronze/fr-esr-implantations_etablissements_d_enseignement_superieur_publics.csv"""
      )

      // Lecture des fichiers CSV en DataFrame
      
      val path_fichier1 = SparkFiles.get("fr-en-baccalaureat-par-academie.csv")
      val path_fichier2 = SparkFiles.get("fr-en-contour-academies-2020.csv")
      val path_fichier3 = SparkFiles.get("fr-esr-implantations_etablissements_d_enseignement_superieur_publics.csv")

      val df_bac = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(path_fichier1)
      val df_contour = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(path_fichier2)
      val df_implantations = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(path_fichier3)
      
      // Normalisation des noms de colonnes
      var dfn_bac = normalizeColumns(df_bac)
      var dfn_contour = normalizeColumns(df_contour)
      var dfn_implantations = normalizeColumns(df_implantations)

      //Normalisation du code_academie dans dfn_implantations (A07 -> 7 / A33 -> 33)
      dfn_implantations = dfn_implantations.withColumn(
        "code_academie",
        regexp_replace(col("code_academie"), "(?i)A0*", "")
      )

      // Normalisation des valeurs de la colonne "sexe" dans dfn_bac pour se limiter à "FEMININ" et "MASCULIN"
      dfn_bac = dfn_bac.withColumn(
        "sexe",
        when(lower(col("sexe")).contains("filles"), "FEMININ")
          .when(lower(col("sexe")).contains("feminin"), "FEMININ")
          .when(lower(col("sexe")).contains("garcons"), "MASCULIN")
          .when(lower(col("sexe")).contains("masculin"), "MASCULIN")
          .otherwise(col("sexe"))
      )

      // Ajout column "insertion_date" avec la date courante pour les 3 dataframes
      dfn_bac = dfn_bac.withColumn("insertion_date", current_timestamp())
      dfn_contour = dfn_contour.withColumn("insertion_date", current_timestamp())
      dfn_implantations = dfn_implantations.withColumn("insertion_date", current_timestamp())

      // Ajout nom libelle region dans dfn_bac via jointure avec dfn_contour
      dfn_bac = dfn_bac.join(
        dfn_contour.select("code_academie", "code_region_2016"),
        dfn_bac("code_academie") === dfn_contour("code_academie"),
        "left"
      ).drop(dfn_contour("code_academie"))

      // Affichage de 20 lignes columns "code_region_2016", "code_academie", "insertion_date" de dfn_bac
      dfn_bac.select("code_region_2016", "code_academie", "insertion_date").show(20)

      // Configuration pour gérer les problèmes de rebasage des dates avec Parquet
      spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

      // Écriture des DataFrames en format Parquet
      dfn_bac.coalesce(1).write.mode("overwrite").parquet("src/main/resources/data/silver/dfn_bac")
      dfn_contour.coalesce(1).write.mode("overwrite").parquet("src/main/resources/data/silver/dfn_contour")
      dfn_implantations.coalesce(1).write.mode("overwrite").parquet("src/main/resources/data/silver/dfn_implantations") 

      println("DataFrames enregistrés en Parquet dans répertoire Silver.")

      // Arrêt de la session Spark
      spark.stop()
  }
}