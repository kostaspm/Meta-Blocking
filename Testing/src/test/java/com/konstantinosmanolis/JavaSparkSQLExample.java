package com.konstantinosmanolis;

import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.expr;
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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.parallel.ParIterableLike.GroupBy;

public class JavaSparkSQLExample {
	private static final int COL_COUNT = 8;

	public static void main(String[] args) {
		JavaSparkSQLExample app = new JavaSparkSQLExample();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("My Spark App").config("spark.master", "local")
				.getOrCreate();

		// Stage 1: Block Filtering
		spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> df = spark.read().json("data/blocks.json");
		df.show();
		df.printSchema();
		spark.udf().register("createPairstest", new createPairsUdfTest(), DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
//		int count = 0;
//		for (int i = 0; i < 4 - 1; i++) {
//			for (int j = i + 1; j < 4; j++) {
//				if (df.col("entities").getItem(i).equals(null) || df.col("entities").getItem(j).equals(null)) {
//					df = df.withColumn("Pair" + count, lit(null));
//					count++;
//				} 
//				else {
//					df = df.withColumn("Pair" + count,
//							array(df.col("entities").getItem(i), df.col("entities").getItem(j)));
//					count++;
//				}
//			}
//		}
//		System.out.println("After Transformations");
//		df.show();
//		df.printSchema();
		
//		df = df.withColumn("entities1", explode(df.col("entities")));
//		df = df.groupBy("entities1").pivot("block").count().na().fill(0);
//		df.show();
		
		df = df.withColumn("Pairs", callUDF("createPairstest", col("entities")));
		df.show(false);
	}
}