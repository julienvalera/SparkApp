package main.scala

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

object SparkApp {
  def main(args: Array[String]) {
    val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    val spark = (SparkSession.builder
      .appName("Velib Data Loader")
      .master("local[*]")
      .getOrCreate())

    // Define a static schema
    val csvSchema = StructType(
      Array(
        StructField("Identifiant station", IntegerType, false),
        StructField("Nom station", StringType, false),
        StructField("Station en fonctionnement", StringType, false),
        StructField("Capacité de la station", IntegerType, false),
        StructField("Nombre bornettes libres", IntegerType, false),
        StructField("Nombre total vélos disponibles", IntegerType, false),
        StructField("Vélos mécaniques disponibles", IntegerType, false),
        StructField("Vélos électriques disponibles", IntegerType, false),
        StructField("Borne de paiement disponible", StringType, false),
        StructField("Retour vélib possible", StringType, false),
        StructField("Actualisation de la donnée", TimestampType, false),
        StructField(
          "Coordonnées géographiques",
          StringType,
          false
        ),
        StructField("Nom communes équipées", StringType, false),
        StructField(
          "Code INSEE communes équipées",
          StringType,
          true
        )
      )
    )

    // Read CSV data
    val dfCsv = (spark.read
      .option("delimiter", ";")
      .option("header", "true")
      .schema(csvSchema)
      .csv(
        spark.conf.get("spark.aws.s3.bucket_uri")
      ))

    // Rename columns
    val dfCsvRenamedColumns = (
      dfCsv
        .withColumnRenamed("Identifiant station", "STATION_ID")
        .withColumnRenamed("Nom station", "STATION_NAME")
        .withColumnRenamed("Station en fonctionnement", "IS_STATION_OPERATING")
        .withColumnRenamed("Capacité de la station", "STATION_CAPACITY")
        .withColumnRenamed("Nombre bornettes libres", "AVAILABLE_TERMINALS")
        .withColumnRenamed("Nombre total vélos disponibles", "AVAILABLE_BIKES")
        .withColumnRenamed(
          "Vélos mécaniques disponibles",
          "AVAILABLE_MECANIC_BIKES"
        )
        .withColumnRenamed(
          "Vélos électriques disponibles",
          "AVAILABLE_ELETRIC_BIKES"
        )
        .withColumnRenamed(
          "Borne de paiement disponible",
          "IS_PAYMENT_TERMINAL_AVAILABLE"
        )
        .withColumnRenamed("Retour vélib possible", "IS_BIKE_RETURN_AVAILABLE")
        .withColumnRenamed(
          "Actualisation de la donnée",
          "DATA_UPDATING_TIMESTAMP"
        )
        .withColumnRenamed("Coordonnées géographiques", "GPS_COORDINATES")
        .withColumnRenamed("Nom communes équipées", "CITY")
        .withColumnRenamed(
          "Code INSEE communes équipées",
          "INSEE_CODE"
        )
    )

    // Snowflake instance parameters
    val sfOptions = Map(
      "sfURL" -> spark.conf.get("spark.snowflake.url"),
      "sfAccount" -> spark.conf.get("spark.snowflake.account"),
      "sfUser" -> spark.conf.get("spark.snowflake.user"),
      "sfPassword" -> spark.conf.get("spark.snowflake.password"),
      "sfDatabase" -> spark.conf.get("spark.snowflake.database"),
      "sfSchema" -> spark.conf.get("spark.snowflake.schema"),
      "sfWarehouse" -> spark.conf.get("spark.snowflake.warehouse"),
    )

    // Write Dataframe data to Snowflake table
    dfCsvRenamedColumns.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", spark.conf.get("spark.snowflake.dbtable"))
      .mode(SaveMode.Overwrite)
      .save()

    spark.stop()
  }
}
