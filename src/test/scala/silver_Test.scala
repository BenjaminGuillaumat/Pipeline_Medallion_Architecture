package com.polytech

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class SilverTest extends AnyFunSuite {

    val spark = SparkSession.builder()
        .appName("SilverTest")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    // Test de la fonction normalizeColumns pour normaliser les chaînes de caractères
    test("normalizeColumns doit normaliser les noms de colonnes") {
        val df = Seq(("École Supérieure", 1)).toDF("Nom_École", "val")
        val normalizedDf = com.polytech.Silver.normalizeColumns(df)
        assert(normalizedDf.columns.contains("nom_ecole"))
    }

    // Test normalisation des codes académie dans dfn_implantations
    test("dfn_implantations code_academie doit être normalisé") {
        val df = Seq(("A07"), ("A33")).toDF("code_academie")
        val dfNorm = df.withColumn("code_academie", regexp_replace(col("code_academie"), "(?i)A0*", ""))
        val codes = dfNorm.collect().map(_.getString(0))
        assert(codes.sameElements(Array("7", "33")))
    }

    // Test normalisation des valeurs de la colonne sexe dans dfn_bac
    test("dfn_bac sexe doit être FEMININ ou MASCULIN") {
        val df = Seq(("Filles"), ("Garçons"), ("Masculin"), ("FEMININ")).toDF("sexe")
        val dfNorm = df.withColumn(
        "sexe",
        when(lower(col("sexe")).contains("filles"), "FEMININ")
        .when(lower(col("sexe")).contains("feminin"), "FEMININ")
        .when(lower(col("sexe")).contains("garcons"), "MASCULIN")
        .when(lower(col("sexe")).contains("masculin"), "MASCULIN")
        .otherwise(col("sexe"))
        )
        val result = dfNorm.select("sexe").collect().map(_.getString(0))
        assert(result.forall(r => r == "FEMININ" || r == "MASCULIN"))
    }

    // Test d'ajout de la colonne insertion_date
    test("insertion_date doit être ajoutée aux DataFrames") {
        val df = Seq((1, "A")).toDF("id", "val")
        val dfWithDate = df.withColumn("insertion_date", current_timestamp())
        assert(dfWithDate.columns.contains("insertion_date"))
    }
    
}
