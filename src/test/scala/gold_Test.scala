package com.polytech

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class GoldTest extends AnyFunSuite {

    val spark = SparkSession.builder()
    .appName("GoldTest")
    .master("local[*]")
    .getOrCreate()

    import spark.implicits._

    // Test du groupBy et count pour le nombre d’établissement supérieur par type d’établissement par région
    test("df_count_eta doit contenir le nombre d'établissement par type et région") {
        val dfImpl = Seq(("7", "Etablissement1", "Public")).toDF("code_academie", "libelle_aca", "type_de_letablissement_siege")
        val dfCont = Seq(("7", "R1")).toDF("code_academie", "code_region_2016")
        val joined = dfImpl.join(dfCont, Seq("code_academie"), "left")
        val grouped = joined.groupBy("libelle_aca", "code_academie", "code_region_2016", "type_de_letablissement_siege").count()
        assert(grouped.columns.contains("count"))
    }

    // Test du calcul du taux de réussite par académie et par genre
    test("taux_academie_genre doit être calculé corresctement") {
        val df = Seq(("Acad1", "7", "R1", "2020", "FEMININ", 100, 80))
            .toDF("libelle_aca", "code_academie", "code_region_2016", "session", "sexe", "nombre_de_presents", "nombre_dadmis_totaux")
        val dfAgg = df.groupBy("libelle_aca", "code_academie", "code_region_2016", "session", "sexe")
            .agg(
                sum("nombre_de_presents").as("total_presents"),
                sum("nombre_dadmis_totaux").as("total_admis")
            )
            .withColumn("taux_academie_genre", round(col("total_admis") / col("total_presents"), 2))
        val taux = dfAgg.select("taux_academie_genre").collect()(0).getDouble(0)
        assert(taux == 0.8)
    }
}
