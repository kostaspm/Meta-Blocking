package com.konstantinosmanolis;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;

public class JavaSparkSQLExample {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("My Spark App").config("spark.master", "local").getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> df = spark.read().json("blocks.json");
		df.show();
		df.foreach(x -> System.out.println(x));
//		df.printSchema();
//		df.select("block").show();
//		df.groupBy("entities").count().show();
	}

}
