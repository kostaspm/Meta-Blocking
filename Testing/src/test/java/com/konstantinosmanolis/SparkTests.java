package com.konstantinosmanolis;

import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.minute;
import static org.apache.spark.sql.functions.second;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.paukov.combinatorics3.Generator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;
import scala.collection.parallel.ParIterableLike.GroupBy;

public class SparkTests {
	private static final int COL_COUNT = 8;

	public static void main(String[] args) {
		SparkTests app = new SparkTests();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("My Spark App").config("spark.master", "local")
				.getOrCreate();

		// Stage 1: Block Filtering
		spark.sparkContext().setLogLevel("ERROR");
		//Dataset<Row> df1 = spark.read().json("data/blocks.json");
		Dataset<Row> df1 = spark.read().format("csv").option("header", "true").load("data/AmazonGoogle/Amazon.csv");
		spark.udf().register("getFrequencies", new getFrequenciesUDF(),
				DataTypes.createArrayType(DataTypes.IntegerType));
		
		
		df1 = df1.withColumn("entityid", row_number().over(Window.orderBy(lit(1)))).drop("description").drop("manufacturer").drop("price").drop("id");
		df1 = df1.withColumn("entityid", df1.col("entityid").cast(DataTypes.LongType));
		df1 = df1.withColumn("title", split(df1.col("title"), " "));
		df1 = df1.withColumn("titleTokens", explode(df1.col("title"))).drop("title");
		df1 = df1.groupBy("titletokens").agg(collect_list("entityid").as("entities"));
		//df1 = df1.withColumn("block", monotonically_increasing_id()).drop("titletokens");
		df1 = df1.withColumn("block", row_number().over(Window.orderBy(lit(1)))).drop("titletokens");
		df1 = df1.withColumn("block", df1.col("block").cast(DataTypes.LongType));
		df1 = df1.select("block", "entities");
		df1.show(false);
		df1.printSchema();
		df1.cache();
		
		
		Dataset<Row> dfmapped = df1.withColumn("ofEntities", size(df1.col("entities")))
				.withColumn("cardinality", expr("int(((ofEntities-1)*ofEntities)/2)")).drop("ofEntities");
		
		/*
		 * Sorting the columns according to cardinality and breaks the arrays of
		 * entities
		 */
		dfmapped = dfmapped.sort(dfmapped.col("cardinality").asc())
				.withColumn("entityId", explode(dfmapped.col("entities"))).drop("entities");
		/*
		 * Groups data by entityID to get rid of duplicates and collect in a
		 * list the blocks that are associated with
		 */
		dfmapped = dfmapped.groupBy("entityId").agg(collect_list(dfmapped.col("block")).alias("AssociatedBlocks"))
				.sort(dfmapped.col("entityId").asc()); // Maybe it will need a monotonically_increasing_id() to get the top N BlocksId
		
		System.out.println("After transformation:");
		dfmapped.show(false);
		dfmapped.printSchema();
		Dataset<Row> dfTest = dfmapped;
		System.out.println(
				"==============================================================================================");
		
		// Stage 2 Preprocessing
		System.out.println("Start of Preprocessing ");
		Dataset<Row> dfPreprocessing = dfmapped.withColumn("BlockId", explode(dfmapped.col("AssociatedBlocks"))).drop("AssociatedBlocks");
		dfPreprocessing = dfPreprocessing.groupBy("BlockId").agg(collect_list("entityId").as("EntityList"));
		dfPreprocessing = dfPreprocessing.withColumn("listSize", size(dfPreprocessing.col("EntityList")));
		dfPreprocessing = dfPreprocessing.filter(col("listSize").geq(2)).drop("listSize");
		dfPreprocessing.show(false);
		System.out.println("End of Preprocessing");
		
		System.out.println("==========================================================================================");
		// Stage 3 Pruning
		// WNP
		
		Dataset<Row> dfWNP = dfPreprocessing.withColumn("EntityId", explode(dfPreprocessing.col("EntityList"))).drop("BlockId");
		dfWNP = dfWNP.select("EntityId", "EntityList");
		dfWNP = dfWNP.withColumnRenamed("EntityList", "CoOccurrenceBag");
		dfWNP.show(false);
		
		dfWNP = dfWNP.groupBy("EntityId").agg(flatten(collect_list("CoOccurrenceBag")).as("CoOccurrenceBag"));
		System.out.println("After Group");
		dfWNP.show(false);
		
		dfWNP = dfWNP.withColumn("numberOfEntities", lit((int)dfWNP.count()));
		dfWNP.show(false);
		dfWNP.printSchema();
		// The array frequencies contains for eve
		dfWNP = dfWNP.withColumn("Frequencies", callUDF("getFrequencies", dfWNP.col("CoOccurrenceBag"), dfWNP.col("numberOfEntities")).cast(DataTypes.createArrayType(DataTypes.LongType)));
		dfWNP = dfWNP.withColumn("SetOfNeighbors", array_distinct(dfWNP.col("CoOccurrenceBag"))).drop("numberOfEntities");
		dfWNP.show(false);
		System.out.println("Get Frequencies Done!");
		
//		spark.udf().register("createPairsTest", new createPairsUdfTest(),
//				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
//
//		df = df.withColumn("pairs", callUDF("createPairsTest", df.col("entities")));
//		df.show(false);
//		df.printSchema();
	}
}