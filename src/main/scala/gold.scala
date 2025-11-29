package com.polytech

import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Gold {

    def main(args: Array[String]): Unit = {
        
        // Initialisation de la session Spark
        val spark = SparkSession.builder()
            .appName("GoldApp")
            .master("local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        // Lecture des DataFrames Parquet depuis le répertoire Silver
        val dfn_bac = spark.read.parquet("src/main/resources/data/silver/dfn_bac")
        val dfn_contour = spark.read.parquet("src/main/resources/data/silver/dfn_contour")
        val dfn_implantations = spark.read.parquet("src/main/resources/data/silver/dfn_implantations")

        // Partie 1 : nombre d’établissement supérieur, par type d’établissement, par région

        val joined = dfn_implantations.join(dfn_contour, Seq("code_academie"), "left")

        val df_count_eta = joined
            .groupBy(
                "libelle_aca",
                "code_academie",
                "code_region_2016",
                "type_de_letablissement_siege"
            )
            .count()
            .withColumnRenamed("count", "nombre_etablissements")
            .orderBy(
                col("libelle_aca").asc,
                col("type_de_letablissement_siege").asc,
                col("nombre_etablissements").asc)

        df_count_eta.show(100)

        //Partie 2 :

        // Renommage colonne code_region_2016 dans dfn_contour pour éviter conflit lors de la jointure
        val df_contour_renamed = dfn_contour
            .withColumnRenamed("code_region_2016", "code_region_2016_contour")

        val df_bac_joined = dfn_bac.join(
            df_contour_renamed.select("code_academie", "libelle_aca", "code_region_2016_contour"),
            Seq("code_academie"),
            "left"
        )

        // 2.1 : pourcentage de réussite masculin et feminin compris pour l’académie en question la même année

        val df_academie = df_bac_joined
            .groupBy("libelle_aca", "code_academie", "code_region_2016", "session", "sexe")
            .agg(
                sum("nombre_de_presents").as("total_presents"),
                sum("nombre_dadmis_totaux").as("total_admis")
            )
            .withColumn("taux_academie_genre", round(col("total_admis") / col("total_presents"), 2))
            .orderBy(
                col("libelle_aca").asc,
                col("session").asc,
                col("sexe").asc)

        df_academie.show(100)

        // 2.2 : pourcentage de réussite national sur l’année pour le même genre

        val df_national_genre = df_bac_joined
            .groupBy("session", "sexe")
            .agg(
                sum("nombre_de_presents").as("nat_presents_genre"),
                sum("nombre_dadmis_totaux").as("nat_admis_genre")
            )
            .withColumn("taux_national_genre", round(col("nat_admis_genre") / col("nat_presents_genre"), 2))
            .orderBy(
                col("session").asc,
                col("sexe").asc)
        
        df_national_genre.show(100)

        // 2.3 : pourcentage de réussite national sur l’année, quelque soit le genre

        val df_national_all = df_bac_joined
            .groupBy("session")
            .agg(
                sum("nombre_de_presents").as("nat_presents_all"),
                sum("nombre_dadmis_totaux").as("nat_admis_all")
            )
            .withColumn("taux_national_all", round(col("nat_admis_all") / col("nat_presents_all"), 2))
            .orderBy(
                col("session").asc)

        df_national_all.show(100)

        // Enregistrement des DataFrames dans PostgreSQL

        val jdbcUrl = "jdbc:postgresql://localhost:5433/db_gold"
        val dbUser = "db_user"
        val dbPassword = "db_pass"
        val dbProperties = new java.util.Properties()
        dbProperties.setProperty("user", dbUser)
        dbProperties.setProperty("password", dbPassword)
        dbProperties.setProperty("driver", "org.postgresql.Driver")

        spark.conf.set("spark.sql.defaultSchema", "public")

        df_count_eta.write.mode("overwrite").jdbc(jdbcUrl, "public.df_count_eta", dbProperties)
        df_academie.write.mode("overwrite").jdbc(jdbcUrl, "public.df_academie", dbProperties)
        df_national_genre.write.mode("overwrite").jdbc(jdbcUrl, "public.df_national_genre", dbProperties)
        df_national_all.write.mode("overwrite").jdbc(jdbcUrl, "public.df_national_all", dbProperties) 

        println("DataFrames écrit dans PostgreSQL avec succès.")
    }
}

