package com.polytech

object MainApp {

  def main(args: Array[String]): Unit = {

    println(" DÉMARRAGE DU PIPELINE SILVER ")
    com.polytech.Silver.main(Array())
    println(" PIPELINE SILVER TERMINÉ")

    println(" DÉMARRAGE DU PIPELINE GOLD ")
    com.polytech.Gold.main(Array())
    println(" PIPELINE GOLD TERMINÉ ")
  }

}
