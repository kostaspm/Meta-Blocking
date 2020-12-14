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
		spark.udf().register("averageWeight", new averageWeightUdf(), DataTypes.DoubleType);
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

		// Stage 3
		// Creates the preffered schema for the stage 3 (Pruning Stage)
		// [node1, node2] | Weight
		Dataset<Row> dfnodes = spark.read().json("data/nodes.json");
		dfnodes = dfnodes
				.withColumn("JaccardWeight",
						expr("double(commonBlocks) / (double(blocksNode1) + double(blocksNode2) - double(commonBlocks))"))
				.drop("blocksNode1").drop("blocksNode2").drop("commonBlocks");
		dfnodes.cache();
		dfnodes.show(false);

		// Weighted Node Pruning
		System.out.println("WNP");
		Dataset<Row> dfnodesWNP = dfnodes
				.withColumn("AllTogether",
						array(struct(dfnodes.col("node").getItem(0),
								struct(dfnodes.col("node").getItem(1), dfnodes.col("JaccardWeight"))),
								struct(dfnodes.col("node").getItem(1),
										struct(dfnodes.col("node").getItem(0), dfnodes.col("JaccardWeight")))))
				.drop("node").drop("JaccardWeight");

		dfnodesWNP = dfnodesWNP.withColumn("AllTogether", explode(dfnodesWNP.col("AllTogether")));
		dfnodesWNP = dfnodesWNP.withColumn("Node", dfnodesWNP.col("AllTogether").getItem("col1"))
							   .withColumn("Node2_Weight", dfnodesWNP.col("AllTogether").getItem("col2")).drop("AllTogether");
		dfnodesWNP = dfnodesWNP.groupBy("Node").agg(collect_list(dfnodesWNP.col("Node2_Weight")).as("Node2_Weight")).sort(dfnodesWNP.col("Node").asc());
		dfnodesWNP = dfnodesWNP.withColumn("Size", size(dfnodesWNP.col("Node2_Weight")))
							   .withColumn("List", dfnodesWNP.col("Node2_Weight.JaccardWeight"));
		dfnodesWNP = dfnodesWNP.withColumn("Average", callUDF("averageWeight", dfnodesWNP.col("List"), dfnodesWNP.col("Size")))
				.drop("Size").drop("List");
		dfnodesWNP = dfnodesWNP.withColumn("Node2_Weight", explode(dfnodesWNP.col("Node2_Weight")));
		dfnodesWNP.show(false);
		System.out.println("=========================================================================================");
		dfnodesWNP.select("Node", "Node2_Weight").filter(col("Node2_Weight.JaccardWeight").gt(dfnodesWNP.col("Average"))).show(false);
		System.out.println("=========================================================================================");

		// dfnodesWNP = dfnodesWNP.withColumn("node",
		// dfnodesWNP.col("AllTogether").getItem(0));
		dfnodesWNP.show(false);
		dfnodesWNP.printSchema();

		// Stage 3 Weighted Edge Pruning
		double meanWeight = dfnodes.select(avg(dfnodes.col("JaccardWeight"))).head().getDouble(0);
		// MPHKE SE SXOLIA GIA NA TO VGALW META TO XREIAZOMAI!
		//dfnodes.select("node","JaccardWeight").filter(col("JaccardWeight").gt(meanWeight)).show(false);

	}

}