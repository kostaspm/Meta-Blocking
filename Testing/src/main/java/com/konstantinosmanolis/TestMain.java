package com.konstantinosmanolis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.monotonically_increasing_id;

public class TestMain {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("My Spark App").config("spark.master", "local")
				.getOrCreate();

		// Stage 1: Block Filtering
		spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> df = spark.read().json("data/blocks.json");
		spark.udf().register("createPairs", new createPairsUdf(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
		spark.udf().register("extractInfo", new extractInfoUdf(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
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
				.sort(dfmapped.col("entityId").asc());

		dfmapped.show(false);
		System.out.println(
				"==============================================================================================");
		// Stage 2: Preprocessing

		// Counts the number of blocks in each Entity and breaks the list of
		// blocks
		dfmapped = dfmapped.withColumn("NumberOfBlocks", size(dfmapped.col("AssociatedBlocks")))
				.withColumn("AssociatedBlocks", explode(dfmapped.col("AssociatedBlocks")));
		dfmapped.show(false);

		dfmapped = dfmapped.sort("AssociatedBlocks");
		dfmapped.show(false);

		// Creates an 1x2 array with entity and number of associated blocks (we
		// need that later too)
		/*
		 * dfmapped = dfmapped .withColumn("entity,numberofBlocks",
		 * array(dfmapped.col("entityId"), dfmapped.col("NumberOfBlocks")))
		 * .drop("entityId").drop("NumberOfBlocks").withColumnRenamed(
		 * "AssociatedBlocks", "BlockId"); dfmapped =
		 * dfmapped.sort(dfmapped.col("BlockId").asc()); dfmapped.show(false);
		 */

		// Groups the columns according the BlockId and creates a list of lists
		// with the EntityId and Number of associated blocks
		/*
		 * Dataset<Row> dfreduced = dfmapped.groupBy("BlockId")
		 * .agg(collect_list(dfmapped.col("entity,numberofBlocks")).alias(
		 * "Entity_BlockNumberList")) .sort(dfmapped.col("BlockId").asc());
		 * dfreduced.show(false); dfreduced.printSchema();
		 */

		// Uses the UDF createPairsUdf and extractInfoUdf and creates 2 columns
		// with the unique pairs in every block and the useful information for
		// the weight computation
		/*
		 * dfreduced = dfreduced.withColumn("PairsList", callUDF("createPairs",
		 * col("Entity_BlockNumberList"))) .withColumn("InfoList",
		 * callUDF("extractInfo", col("Entity_BlockNumberList")));
		 * dfreduced.show(false);
		 */
		
		// Stage 3 Weighted Node Pruning
		Dataset<Row> dfnodes = spark.read().json("data/nodes.json");
		dfnodes = dfnodes
				.withColumn("JaccardWeight",
						expr("double(commonBlocks) / (double(blocksNode1) + double(blocksNode2) - double(commonBlocks))"))
				.drop("blocksNode1").drop("blocksNode2").drop("commonBlocks");
		dfnodes.cache();
		dfnodes.show(false);
		dfnodes.printSchema();
		
		double meanWeight = dfnodes.select(avg(dfnodes.col("JaccardWeight"))).head().getDouble(0);
		
		Dataset<Row> dfnodesWNP = dfnodes.withColumn("node", explode(dfnodes.col("node")));
		dfnodesWNP.show(false);
		//============= еимаи кахос==================dfnodes.select("node","JaccardWeight").filter(col("JaccardWeight").gt(meanWeight)).show(false);
		
		// Stage 3 Weighted Edge Pruning
		long numberOfNodes = dfnodes.count();
		dfnodes.select("node","JaccardWeight").filter(col("JaccardWeight").gt(meanWeight/numberOfNodes)).show(false);
		
		
		// Paper's method
		dfnodes = dfnodes.withColumn("node", explode(dfnodes.col("node")));
		dfnodes = dfnodes.withColumn("entity_weight", struct(dfnodes.col("node"), dfnodes.col("JaccardWeight"))).drop("JaccardWeight");
		
		//dfnodes = dfnodes.withColumn("entitites", explode(dfnodes.col("node")));
		//dfnodes = dfnodes.select(dfnodes.col("node").getItem(0).as("ID1"), dfnodes.col("node").getItem(1).as("ID2"), dfnodes.col("JaccardWeight"));
		
		//dfnodes = dfnodes.withColumn("first", struct(dfnodes.col("ID1"), dfnodes.col("JaccardWeight"))).withColumn("second", struct(dfnodes.col("ID2"), dfnodes.col("JaccardWeight")));
//		
//		dfnodes = dfnodes.withColumn("ID1_ID2Weight", struct(dfnodes.col("ID1"), dfnodes.col("second"))).withColumn("ID2_ID1Weight", struct(dfnodes.col("ID2"), dfnodes.col("first")));
//		dfnodes = dfnodes.select(dfnodes.col("ID1"), dfnodes.col("second"), dfnodes.col("ID2"), dfnodes.col("first"));
		dfnodes.show(false);
		
		//dfnodes.printSchema();
		
		// average calculation
		// dfnodes.select(avg(dfnodes.col("second").getItem("JaccardWeight"))).show();
		
		
	}

}