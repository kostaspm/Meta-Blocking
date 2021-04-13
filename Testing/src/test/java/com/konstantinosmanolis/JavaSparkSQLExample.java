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
import java.util.Arrays;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.paukov.combinatorics3.Generator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;
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
		spark.udf().register("createPairsTest", new createPairsUdfTest(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));

		df = df.withColumn("pairs", callUDF("createPairsTest", df.col("entities")));
		df.show(false);
		df.printSchema();
	}
}

//class createPairsUdfTest implements UDF1<WrappedArray<Long>, ArrayList<long[]>> {
//	private static Logger log = LoggerFactory.getLogger(createPairsUdfTest.class);
//
//	private static final long serialVersionUID = -21621750L;
//
//	@Override
//	public ArrayList<long[]> call(WrappedArray<Long> entities) throws Exception {
//		log.debug("-> call({}, {})", entities);
//		
//		ArrayList<long[]> pairs = new ArrayList<long[]>();
//		Combination.printCombination(entities, entities.length(), 2, pairs);
//
//		return pairs;
//	}
//
//}
//
//class Combination {
//
//	/*
//	 * arr[] ---> Input Array data[] ---> Temporary array to store current
//	 * combination start & end ---> Staring and Ending indexes in arr[] index
//	 * ---> Current index in data[] r ---> Size of a combination to be printed
//	 */
//	/*
//	 * arr[] ---> Input Array data[] ---> Temporary array to store current
//	 * combination start & end ---> Staring and Ending indexes in arr[] index
//	 * ---> Current index in data[] r ---> Size of a combination to be printed
//	 */
//	static void combinationUtil(WrappedArray<Long> arr, long[] data, int start, int end, int index, int r, ArrayList<long[]> pairs) {
//		// Current combination is ready to be printed, print it
//		if (index == r) {
//			long[] combination = data.clone();
//			pairs.add(combination);
//			return;
//		}
//
//		// replace index with all possible elements. The condition
//		// "end-i+1 >= r-index" makes sure that including one element
//		// at index will make a combination with remaining elements
//		// at remaining positions
//		for (int i = start; i <= end && end - i + 1 >= r - index; i++) {
//			data[index] = arr.apply(i);
//			combinationUtil(arr, data, i + 1, end, index + 1, r, pairs);
//		}
//	}
//
//	// The main function that prints all combinations of size r
//	// in arr[] of size n. This function mainly uses combinationUtil()
//	static void printCombination(WrappedArray<Long> arr, int n, int r, ArrayList<long[]> pairs) {
//		// A temporary array to store all combination one by one
//		long[] data = new long[r];
//		//ArrayList<Long> data = new ArrayList<Long>(r);
//
//		// Print all combination using temporary array 'data[]'
//		combinationUtil(arr, data, 0, n - 1, 0, r, pairs);
//	}
//}