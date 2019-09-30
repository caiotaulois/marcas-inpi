package com.bigdata.spark

import com.databricks.spark.xml._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

object Marcas {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("Por favor, inserir IP elasticsearch, porta elasticsearch e caminho para XMLs.")
      return
    }
    val node = args(0)
    val port = args(1)
    val path = args(2)

    val spark = iniciarSparkSession(node, port)
    val marcas = spark.read.option("rowTag", "processo").xml(path + "\\*.xml")
    val classes = spark.read.option("header", "true").csv(path + "\\classe-nice.csv").as("classes")
    aprovadosPorClasse(marcas, classes)
    registrosPorPaisEstrangeiro(marcas)
    procuradores(marcas)
    estadosSemProcurador(marcas)
    duracaoProcessosEstadosNoBrasil(marcas)
  }

  def iniciarSparkSession(node: String, port: String): SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Marcas INPI")
      .config("spark.es.nodes", node)
      .config("spark.es.port", port)
      .config("es.nodes.wan.only", "true")
      .getOrCreate()
  }

  def aprovadosPorClasse(marcas: DataFrame, classesNice: DataFrame): Unit = {
    marcas
      .withColumn("despacho", explode(col("despachos.despacho")))
      .filter(col("despacho._codigo") === "IPAS158")
      .groupBy("classe-nice._codigo")
      .count()
      .withColumnRenamed("_codigo", "codigoClasse")
      .withColumnRenamed("count", "registros")
      .as("registrosPorClasse")
      .join(classesNice, col("registrosPorClasse.codigoClasse") === col("classes.codigo"))
      .select("registrosPorClasse.codigoClasse", "classe", "registros")
      .saveToEs("classes/classe", Map("es.mapping.id" -> "codigoClasse"))
  }

  def registrosPorPaisEstrangeiro(marcas: DataFrame): Unit = {
    marcas
      .filter(col("titulares.titular._pais").isNotNull)
      .filter(col("titulares.titular._pais") =!= "BR")
      .groupBy("titulares.titular._pais")
      .count()
      .withColumnRenamed("_pais", "pais")
      .withColumnRenamed("count", "registros")
      .select("pais", "registros")
      .saveToEs("paises/pais", Map("es.mapping.id" -> "pais"))
  }

  def procuradores(marcas: DataFrame): Unit = {
    marcas
      .groupBy("procurador")
      .count()
      .withColumnRenamed("_procurador", "procurador")
      .withColumnRenamed("count", "registros")
      .select("procurador", "registros")
      .saveToEs("procuradores/procurador", Map("es.mapping.id" -> "procurador"))
  }

  def estadosSemProcurador(marcas: DataFrame): Unit = {
    marcas
      .filter(col("procurador").isNull)
      .filter(col("titulares.titular._pais")==="BR")
      .groupBy("titulares.titular._uf")
      .count()
      .withColumnRenamed("_uf", "uf")
      .withColumnRenamed("count", "registros")
      .select("uf", "registros")
      .saveToEs("ufs/uf", Map("es.mapping.id" -> "uf"))
  }

  def duracaoProcessosEstadosNoBrasil(marcas: DataFrame): Unit = {
    marcas
      .filter(col("_data-deposito").isNotNull)
      .filter(col("_data-concessao").isNotNull)
      .filter(col("titulares.titular._pais")==="BR")
      .withColumn("dataDeposito", to_date(unix_timestamp(col("_data-deposito"), "dd/MM/yyyy").cast("timestamp")))
      .withColumn("dataConcessao", to_date(unix_timestamp(col("_data-concessao"), "dd/MM/yyyy").cast("timestamp")))
      .withColumn("duracaoProcesso", datediff(col("dataConcessao"), col("dataDeposito")))
      .groupBy("titulares.titular._uf")
      .avg("duracaoProcesso")
      .withColumnRenamed("_uf", "uf")
      .withColumn("duracaoProcesso", round(col("avg(duracaoProcesso)")))
      .select("uf", "duracaoProcesso")
      .saveToEs("processos/duracao", Map("es.mapping.id" -> "uf"))
  }
}
