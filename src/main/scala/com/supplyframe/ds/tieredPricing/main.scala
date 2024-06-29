package com.supplyframe.ds.tieredPricing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, _}

import scala.util.Random
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.UserDefinedFunction

object main {

  def saveFiles(df: DataFrame, fn: String, TYPE: String): Unit = {
    if (TYPE == "avro") {
      df.write.format("avro").mode("overwrite").save(fn)
    } else if (TYPE == "csv") {
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv(fn + "_csv")
    } else if (TYPE == "parquet") {
      df.write.format("parquet").mode("overwrite").save(fn)
    } else {
      df.write.format("avro").mode("overwrite").save(fn)
      df.coalesce(1).write.mode("overwrite").option("header", "true").csv(fn + "_csv")
    }
  }

  def explodePriceQuantity(dataFrame: DataFrame, PRICE: Boolean): DataFrame = {

    PRICE match {
      case true =>
        dataFrame.withColumn("parts_priceSingleQty", explode(col("parts_price"))
        ).withColumn("price", col("parts_priceSingleQty.price")
        ).withColumn("quantity", col("parts_priceSingleQty.quantity")
        ).drop("parts_priceSingleQty"
        )
      case false =>
        dataFrame.withColumn("parts_priceSingleQty", explode(col("parts_price"))
        ).withColumn("quantity", col("parts_priceSingleQty.quantity")
        ).drop("parts_priceSingleQty"
        )
    }
  }

  def fillInNaColumn(columnName: String) = {
    when(col(columnName).isNull || col(columnName) === "null" || col(columnName) === "", "Unspecified").otherwise(col(columnName))
  }

  def addSuffixToColumnNamesExceptGroupCols(df: DataFrame, groupCols: Seq[String], suffix: String): DataFrame = {

    // Identify columns that need to be renamed (those not in groupCols)
    val columnsToRename = df.columns.diff(groupCols)

    // Create a sequence of tuples for columns that need renaming
    val newColumns = columnsToRename.map(colName => colName -> (colName + "_" + suffix))

    // Rename the columns in the DataFrame
    val renamedDF = newColumns.foldLeft(df) { (tempDF, colPair) =>
      tempDF.withColumnRenamed(colPair._1, colPair._2)
    }

    renamedDF
  }

  def findOutliers(df: DataFrame, groupCols: Seq[String], colName: String): DataFrame = {
    val groupedDF = df.groupBy(groupCols.map(col): _*).agg(
      expr(s"percentile_approx($colName, 0.5)").as("median"),
      expr(s"percentile_approx($colName, 0.25)").as("q1"),
      expr(s"percentile_approx($colName, 0.75)").as("q3"),
      mean(colName).as("mean"),
      stddev(colName).as("stddev"),
      count(colName).as("count")
    ).withColumn("IQR", col("q3") - col("q1")
    ).withColumn("std_outlier_lb",
      when(col("count") === 1, lit(Double.NegativeInfinity)).otherwise(
        col("mean") - lit(5) * col("stddev")
      )
    ).withColumn("std_outlier_up",
      when(col("count") === 1, lit(Double.PositiveInfinity)).otherwise(
        col("mean") + lit(5) * col("stddev")
      )
    ).withColumn("iqr_outlier_lb",
      when(col("count") === 1, lit(Double.NegativeInfinity)).otherwise(
        col("median") - lit(5) * col("IQR")
      )
    ).withColumn("iqr_outlier_up",
      when(col("count") === 1, lit(Double.PositiveInfinity)).otherwise(
        col("median") + lit(5) * col("IQR")
      )
    ).withColumn("outlier_lb", least(col("iqr_outlier_lb"), col("median") * lit(0.1))
    ).withColumn("outlier_up", greatest(col("iqr_outlier_up"), col("median") * lit(10))
    )

    addSuffixToColumnNamesExceptGroupCols(groupedDF.drop("count"), groupCols, colName)
  }

  def savePartSamples(spark: SparkSession, monthString: String, dataDir: String): (String, String) = {

    //      val monthString = "2024/05"
    val eCommerceDir = "/prod/etl/OrthogonalAggregations/EcommerceLayer/" + monthString
    val nTopClick = 1500

    // UDF
    val randomSampleUDF = udf((partNumbers: Seq[String]) => {
      val sampleSize = 10
      Random.shuffle(partNumbers).take(sampleSize)
    })

    // part samples
    val fcEcommerce = spark.read.format("parquet").load(eCommerceDir
    ).filter(col("website_name") === "FindChips"
    )

    val topClickParts = fcEcommerce.groupBy("part_number", "manufacturer_name").agg(
      sum(col("clicks")).alias("click_count")
    ).orderBy(desc("click_count")).limit(nTopClick)

    //      val randomParts = fcEcommerce.groupBy("sf_class", "sf_category"
    //      ).agg(collect_list("part_number").as("part_numbers")
    //      ).withColumn("sampled_part_numbers", randomSampleUDF(col("part_numbers"))
    //      ).withColumn("part_number", explode(col("sampled_part_numbers")).as("part_number")
    //      ).select("sf_class", "sf_category", "part_number"
    //      )

    val windowSpec = Window.partitionBy("sf_class", "sf_category").orderBy(rand())
    val withRowNum = fcEcommerce.withColumn("row_num", row_number().over(windowSpec))
    val randomParts = withRowNum.filter(col("row_num") <= 10).select("sf_class", "sf_category", "part_number")

    val saveFileName1 = dataDir + "/part_samples_" + monthString.replace("/", "") + "/top_" + nTopClick.toString + "_clicks"
    val saveFileName2 = dataDir + "/part_samples_" + monthString.replace("/", "") + "/uniformly_random10_each_category"
    saveFiles(topClickParts, saveFileName1, "parquet")
    saveFiles(randomParts, saveFileName2, "parquet")

    (saveFileName1, saveFileName2)
  }

  def getFcMacroFilteredDf(spark: SparkSession): DataFrame = {

    // constant
    val fcApiPath = "/prod/etl/OrthogonalAggregations/NetworkTrends/FcapiScraper/Serialized/2024/??"
    val columnsToFillNa = Seq("parts_custom_dateCode", "parts_dateCode",
      "parts_distributorItemNo", "parts_packageType", "parts_distributorItemNo")

    // read data
    var fc = spark.read.format("parquet").load(fcApiPath
    ).withColumn("metadata_requestData_part", upper(col("metadata_requestData_part"))
    ).withColumn("parts_partNumber", upper(col("parts_partNumber"))
    ).withColumn("region", when(lower(col("parts_stockIndicator")).contains("americas"), "Americas").otherwise(
      when(lower(col("parts_stockIndicator")).contains("europe"), "Europe").otherwise(
        when(lower(col("parts_stockIndicator")).contains("asia"), "Asia").otherwise("Unspecified")))
    ).withColumn("metadata_roundInMonth",
      when(dayofmonth(col("metadata_date")) < 15, 1).otherwise(2)
    ).withColumn("metadata_roundInYear",
      split(col("metadata_date"), "-").getItem(1).cast("int")
    ).withColumn("metadata_monthRound", lit(2) * (col("metadata_roundInYear") - 1) + col("metadata_roundInMonth")
    )

    fc = fc.withColumn(
      "parts_masterDistributorItemNo",
        concat_ws(
          "|",
          concat(lit("distributor"), col("distributor_name")),
          concat(lit("din="), col("parts_distributorItemNo")),
          concat(lit("package="), col("parts_packageType")),
          concat(lit("custom_dateCode="), col("parts_custom_dateCode")),
          concat(lit("dateCode="), col("parts_dateCode")),
          concat(lit("minQty="), col("parts_minimumQuantity").cast("string")),
          concat(lit("multiple="), col("parts_packageMultiple").cast("string"))
        )
    )

    fc = columnsToFillNa.foldLeft(fc) { (tempDf, colName) =>
      tempDf.withColumn(colName, fillInNaColumn(colName))
    }

    // remove out of stock, rfq, Europe, Asia, null prices, empty call results
    val filteredFc = fc.filter(col("parts_buyNowUrl").isNotNull
    ).filter(col("parts_stock") > 0
    ).filter(col("parts_price").isNotNull
    ).filter(size(col("parts_price")) > 0
    ).filter(col("region") =!= "Europe"
    ).filter(col("region") =!= "Asia"
    )

    filteredFc

  }

  def filterOutlierApiCalls(filteredFc: DataFrame, SAVE: Boolean, dataDir: String): DataFrame = {

    // find the median price for each API call
    val histExplodedPartsPrice = explodePriceQuantity(
      filteredFc.select("region", "metadata_monthRound", "metadata_requestData_part", "parts_manufacturer", "distributor_name", "parts_price"),
      PRICE = true
    ).drop("quantity")
    val histApiPrice = histExplodedPartsPrice.groupBy(
      "region", "metadata_monthRound", "metadata_requestData_part", "parts_manufacturer", "distributor_name", "parts_price"
    ).agg(
      expr(s"percentile_approx(price, 0.5)").as("median_price_apiCall"),
      max("price").alias("max_price_apiCall")
    )

    // compare median API price to the median overall price, compute ratio
    val histOutlierBenchmark = histApiPrice.groupBy(
      "region", "metadata_monthRound", "metadata_requestData_part", "parts_manufacturer").agg(
      expr(s"percentile_approx(median_price_apiCall, 0.5)").as("median_price_part"),
    ).join(histApiPrice, Seq("region", "metadata_monthRound", "metadata_requestData_part", "parts_manufacturer"), "right"
    ).withColumn("ratio", col("median_price_apiCall") / col("median_price_part")
    ).withColumn("ratio_maxApiPrice_over_meanPrice", col("max_price_apiCall") / col("median_price_part")
    )

    val filterOutHist = histOutlierBenchmark.filter(
      ((col("ratio") > 20) || col("ratio") < 1 / 20) ||
        ((col("ratio_maxApiPrice_over_meanPrice") > 50) || col("ratio_maxApiPrice_over_meanPrice") < 1 / 50)
    )

    if (SAVE) {
      saveFiles(filterOutHist, dataDir + "/FcApi2024_distributor_prices_latest_v2/filtered_outlier_Api_calls", "parquet")
    }

    histOutlierBenchmark

  }

  def getTopClickSamples(spark: SparkSession, dataDir: String, saveFileName1: String) = {

    // read part samples
//    val saveFileName1 = dataDir + "/part_samples_202405/top_1500_clicks"
    val topClickParts = spark.read.format("parquet").load(saveFileName1)
    // read price data
    val current = spark.read.format("parquet").load(dataDir + "/FcApi2024_distributor_prices_latest_v2/current_full")
    val histRange = spark.read.format("parquet").load(dataDir + "/FcApi2024_distributor_prices_latest_v2/last_quarter_range_full")

    // compute distinct distr/mfr
    val partCarryingInfo = current.groupBy("metadata_requestData_part").agg(
      countDistinct("distributor_name").alias("number_of_carrier"),
      countDistinct("parts_manufacturer").alias("number_of_mfr")
    )

    // find prices
    val topClickPartPrices = current.join(
      topClickParts.withColumnRenamed("manufacturer_name", "parts_manufacturer"
      ).withColumnRenamed("part_number", "metadata_requestData_part"),
      Seq("metadata_requestData_part", "parts_manufacturer"), "inner"
    ).distinct().join(partCarryingInfo, Seq("metadata_requestData_part"), "left")
    //    saveFiles(topClickPartPrices, dataDir + "/FcApi2024_distributor_prices_latest_v2/current_sample_top_clicks_parts", "csv")

    val topClickPartPricesHist = histRange.join(
      topClickParts.withColumnRenamed("manufacturer_name", "parts_manufacturer"
      ).withColumnRenamed("part_number", "metadata_requestData_part"),
      Seq("metadata_requestData_part", "parts_manufacturer"), "inner"
    ).distinct().drop("quantity_base10")
    //    saveFiles(topClickPartPricesHist, dataDir + "/FcApi2024_distributor_prices_latest_v2/last_quarter_sample_top_clicks_parts", "csv")

    val commonCols = topClickPartPrices.columns.intersect(topClickPartPricesHist.columns).toSeq
    val topClickPartPricesAll = topClickPartPrices.join(topClickPartPricesHist, commonCols, "left")
    saveFiles(topClickPartPricesAll, dataDir + "/FcApi2024_distributor_prices_latest_v2/top_clicks_parts", "csv")

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("yarn")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.shuffle.blockTransferService", "nio")
    conf.set("spark.ui.showConsoleProgress", "true")
    conf.set("spark.shuffle.service.enabled", "true")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.sql.avro.compression.codec", "snappy")
    conf.set("spark.speculation", "false")
    conf.set("spark.hadoop.mapreduce.map.speculative", "false")
    conf.set("spark.hadoop.mapreduce.reduce.speculative", "false")

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // UDF
    val fillPriceUDF: UserDefinedFunction = udf((partsPrice: Seq[Row], quantity: Long) => {

      if (partsPrice == null || partsPrice.isEmpty) {
        // Handle the case where items are null or empty
        -1
      }

      val sortedPartsPrice = partsPrice.sortBy(_.getAs[Long]("quantity"))
      val filteredPrices = sortedPartsPrice.filter(_.getAs[Long]("quantity") <= quantity)
      if (filteredPrices.isEmpty) {
        //        sortedPartsPrice.head.getAs[Double]("price") // If no smaller quantity, get the current price
        -1
      } else {
        val nextPrice = sortedPartsPrice.filter(_.getAs[Long]("quantity") > quantity).headOption
        if (nextPrice.isDefined) {
          filteredPrices.last.getAs[Double]("price") // Get the last price with smaller quantity
        } else {
          sortedPartsPrice.last.getAs[Double]("price") // If no greater quantity, get the last price
        }
      }
    })

    // directories
    val projectDir = "/user/qye/Price_estimation_real_time"
    val dataDir = projectDir + "/data"

    // constants
    val partsConsistentCols = Seq("parts_manufacturer", "metadata_requestData_part",
      "parts_packageType", "parts_custom_dateCode", "parts_dateCode",
      "parts_distributorItemNo", "parts_masterDistributorItemNo")
    val partsVaryCols = Seq("parts_minimumQuantity", "parts_packageMultiple", "parts_stock", "parts_price")
    val groupCols = Seq("region", "metadata_monthRound", "distributor_name")

    var tempCols = groupCols ++ partsConsistentCols ++ partsVaryCols
    val filteredFc = getFcMacroFilteredDf(spark).select(tempCols.head, tempCols.tail: _*)

    // filter outliers of API calls: median_API_price / median_overall_price
    val histOutlierBenchmark = filterOutlierApiCalls(filteredFc, SAVE = true, dataDir = dataDir)
    val reasonableRangeFc = filteredFc.join(histOutlierBenchmark,
      Seq("region", "metadata_monthRound", "metadata_requestData_part",
        "parts_manufacturer", "distributor_name", "parts_price"), "left"
    ).filter((col("ratio") < 20) && (col("ratio") >= 1 / 20)
    ).filter((col("ratio_maxApiPrice_over_meanPrice") <= 50)
      && (col("ratio_maxApiPrice_over_meanPrice") >= 1 / 50)
    ).drop("median_price_part", "median_price_apiCall", "ratio", "ratio_maxApiPrice_over_meanPrice"
    )

    // separate current and history
    val maxValue = filteredFc.agg(max("metadata_monthRound")).collect()(0)(0)
    val currentPricing = reasonableRangeFc.filter(col("metadata_monthRound") === maxValue)
    val histPricing = reasonableRangeFc.filter(col("metadata_monthRound") >= lit(maxValue) - 6)

    // quantities of parts in history
    tempCols = groupCols.filter(_ != "metadata_monthRound") ++ partsConsistentCols :+ "quantity"
    val histQtyDistMfrPart = explodePriceQuantity(histPricing, PRICE = false).select(tempCols.head, tempCols.tail: _*).distinct()
    tempCols = groupCols.filter(_ != "metadata_monthRound") ++ partsConsistentCols :+ "parts_stock"
    val currentStockQty = currentPricing.select(tempCols.head, tempCols.tail: _*).withColumnRenamed("parts_stock", "quantity").distinct()

    // find prices at each quantity
    val hist0 = histPricing.join(currentStockQty, currentStockQty.columns.filter(_ != "quantity"), "left"
    ).withColumnRenamed("quantity", "quantity_histQtyMfrPart"
    ).join(histQtyDistMfrPart, histQtyDistMfrPart.columns.filter(_ != "quantity"), "left"
    ).withColumnRenamed("quantity", "quantity_currentStock"
    ).withColumn("price_histQtyMfrPart", fillPriceUDF(col("parts_price"), col("quantity_histQtyMfrPart"))
    ).withColumn("price_currentStock", fillPriceUDF(col("parts_price"), col("quantity_currentStock"))
    )

    tempCols = groupCols ++ partsConsistentCols ++ partsVaryCols ++ Seq("price_histQtyMfrPart", "quantity_histQtyMfrPart")
    val histSub1 = hist0.select(tempCols.head, tempCols.tail: _*).withColumnRenamed("price_histQtyMfrPart", "price"
    ).withColumnRenamed("quantity_histQtyMfrPart", "quantity"
    )
    tempCols = groupCols ++ partsConsistentCols ++ partsVaryCols ++ Seq("price_currentStock", "quantity_currentStock")
    val histSub2 = hist0.select(tempCols.head, tempCols.tail: _*).withColumnRenamed("price_currentStock", "price"
    ).withColumnRenamed("quantity_currentStock", "quantity"
    )
    val hist = histSub1.union(histSub2).filter(col("price") > 0
    ).filter((col("parts_stock") >= col("quantity")) || col("parts_stock").isNull
    ).filter((col("parts_minimumQuantity") <= col("quantity")) || col("parts_minimumQuantity").isNull
    ).distinct()

    // historical ranges
    val histRange = hist.groupBy((partsConsistentCols ++ Seq("distributor_name", "region", "quantity")).map(col): _*).agg(
      min(col("price")).alias("min_price"),
      max(col("price")).alias("max_price"),
      expr(s"percentile_approx(price, 0.25)").as("q1_price"),
      expr(s"percentile_approx(price, 0.75)").as("q3_price"),
      expr(s"percentile_approx(price, 0.5)").as("median_price"),
      mean(col("price")).alias("mean_price"),
      stddev(col("price")).alias("std_price"),
    )

    // price at quantity
    val current0 = explodePriceQuantity(currentPricing, PRICE = true)
    // price at parts_stock
    val currentStockPrice = currentPricing.withColumn("price", fillPriceUDF(col("parts_price"), col("parts_stock"))
    ).withColumn("quantity", col("parts_stock"))

    // full current prices
    val current = current0.union(currentStockPrice
    ).filter((col("parts_stock") >= col("quantity")) || col("parts_stock").isNull
    ).filter((col("parts_minimumQuantity") <= col("quantity")) || col("parts_minimumQuantity").isNull
    ).filter(col("price") > 0
    ).filter(col("quantity") > 0
    ).drop("parts_price").distinct()

    saveFiles(current, dataDir + "/FcApi2024_distributor_prices_latest_v2/current_full", "parquet")
    saveFiles(histRange, dataDir + "/FcApi2024_distributor_prices_latest_v2/last_quarter_range_full", "parquet")

    // generate part samples (if not availabe)
    // val (saveFileName1, saveFileName2) = savePartSamples(spark, "2024/05", dataDir)
    val saveFileName1 = dataDir + "/part_samples_202405/top_1500_clicks"

    // save price for top click parts
    getTopClickSamples(spark, dataDir, saveFileName1)

  }
}
