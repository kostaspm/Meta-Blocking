package comparisonBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.array_sort;
import static org.apache.spark.sql.functions.array_intersect;
import static org.apache.spark.sql.functions.struct;

import java.util.ArrayList;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class ComparisonMain {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().appName("Comparison Based Strategy").config("spark.master", "local")
				.getOrCreate();
		
		spark.sparkContext().setLogLevel("ERROR");
		//spark.udf().register("extractInfo", new extractInfoUdf(),
		//		DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
		
		Dataset<Row> df = spark.read().json("data/blocks.json");
		System.out.println("Before transformation:");
		df.show(false);
		
		/*
		 * Calculates the cardinality of each block and creates a new column to
		 * save the data
		 */
		Dataset<Row> dfmapped = df.withColumn("ofEntities", size(df.col("entities")))
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
		System.out.println(
				"==============================================================================================");
		
		// Stage 2 Preprocessing
		System.out.println("Start of Preprocessing ");
		Dataset<Row> dfPreprocessing = dfmapped.withColumn("AssociatedBlocks", array_sort(dfmapped.col("AssociatedBlocks")));
		dfPreprocessing.show(false);
		
		dfPreprocessing = dfPreprocessing.withColumn("BlockId", explode(dfPreprocessing.col("AssociatedBlocks"))).withColumn("EntityId_AssociatedBlocks", struct(dfPreprocessing.col("entityId"), dfPreprocessing.col("AssociatedBlocks"))).drop("entityId").drop("AssociatedBlocks").sort("BlockId");
		dfPreprocessing = dfPreprocessing.groupBy("BlockId").agg(collect_list("EntityId_AssociatedBlocks").as("EntityId_AssociatedBlocks"));
		dfPreprocessing = dfPreprocessing.withColumn("listSize", size(dfPreprocessing.col("EntityId_AssociatedBlocks")));
		dfPreprocessing = dfPreprocessing.filter(col("listSize").geq(2)).select("BlockId", "EntityId_AssociatedBlocks");
		System.out.println("After Preprocessing");
		dfPreprocessing.show(false);
		dfPreprocessing.printSchema();
		System.out.println(
				"==============================================================================================");
		
		// Stage 3 Pruning
		// Weighted Node Pruning
//		Dataset<Row> dfWNP = dfPreprocessing.withColumn("Struct1", dfPreprocessing.col("list").getItem(0)).withColumn("Struct2", dfPreprocessing.col("list").getItem(1));
//		dfWNP.show(false);
//		dfWNP.printSchema();
		
		Dataset<Row> dfWNP = dfPreprocessing.withColumn("EntityId_AssociatedBlocks", explode(dfPreprocessing.col("EntityId_AssociatedBlocks")));
		dfWNP.createOrReplaceTempView("data");
		//dfWNP = spark.sql("SELECT list FROM data WHERE BlockId = 1");
		//dfWNP = dfWNP.select("list.entityId").where(dfWNP.col("BlockId") == 1);
		dfWNP.show(false);
		dfWNP.printSchema();
		dfWNP = dfWNP.select("BlockId","EntityId_AssociatedBlocks.AssociatedBlocks");
		dfWNP = dfWNP.groupBy("BlockId").agg(collect_list(dfWNP.col("AssociatedBlocks")).as("Blocks"));
		dfWNP.show(false);
		
		
	
//		dfWNP = dfWNP.withColumn("Block1", dfWNP.col("Struct1.AssociatedBlocks")).withColumn("Block2", dfWNP.col("Struct2.AssociatedBlocks")).drop("Struct1").drop("Struct2");
//		dfWNP = dfWNP.withColumn("JaccardWeight", expr("int(size(array_intersect(Block1, Block2)))/int(size(Block1) + size(Block2) - int(size(array_intersect(Block1, Block2))))"));
//		dfWNP.show(false);
		
	}
}
class createPairsUdfTest implements UDF1<WrappedArray<WrappedArray<Long>>, ArrayList<long[]>> {
	private static Logger log = LoggerFactory.getLogger(createPairsUdfTest.class);

	private static final long serialVersionUID = -21621750L;

	@Override
	public ArrayList<long[]> call(WrappedArray<WrappedArray<Long>> entities) throws Exception {
		log.debug("-> call({}, {})", entities);
		
		ArrayList<long[]> pairs = new ArrayList<long[]>();
		Combination.printCombination(entities, entities.length(), 2, pairs);

		return pairs;
	}

}

class Combination {

	/*
	 * arr[] ---> Input Array data[] ---> Temporary array to store current
	 * combination start & end ---> Staring and Ending indexes in arr[] index
	 * ---> Current index in data[] r ---> Size of a combination to be printed
	 */
	/*
	 * arr[] ---> Input Array data[] ---> Temporary array to store current
	 * combination start & end ---> Staring and Ending indexes in arr[] index
	 * ---> Current index in data[] r ---> Size of a combination to be printed
	 */
	static void combinationUtil(WrappedArray<WrappedArray<Long>> arr, long[] data, int start, int end, int index, int r, ArrayList<long[]> pairs) {
		// Current combination is ready to be printed, print it
		if (index == r) {
			long[] combination = data.clone();
			pairs.add(combination);
			return;
		}

		// replace index with all possible elements. The condition
		// "end-i+1 >= r-index" makes sure that including one element
		// at index will make a combination with remaining elements
		// at remaining positions
		for (int i = start; i <= end && end - i + 1 >= r - index; i++) {
			data[index] = arr.apply(i).apply(i);
			combinationUtil(arr, data, i + 1, end, index + 1, r, pairs);
		}
	}

	// The main function that prints all combinations of size r
	// in arr[] of size n. This function mainly uses combinationUtil()
	static void printCombination(WrappedArray<WrappedArray<Long>> arr, int n, int r, ArrayList<long[]> pairs) {
		// A temporary array to store all combination one by one
		long[] data = new long[r];
		//ArrayList<Long> data = new ArrayList<Long>(r);

		// Print all combination using temporary array 'data[]'
		combinationUtil(arr, data, 0, n - 1, 0, r, pairs);
	}
}
