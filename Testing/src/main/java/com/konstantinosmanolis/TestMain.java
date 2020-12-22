package com.konstantinosmanolis;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.sort_array;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.slice;
import static org.apache.spark.sql.functions.struct;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
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
		
		 dfmapped = dfmapped .withColumn("entity,numberofBlocks", array(dfmapped.col("entityId"), dfmapped.col("NumberOfBlocks"))).drop("entityId").drop("NumberOfBlocks").withColumnRenamed("AssociatedBlocks", "BlockId"); 
		 dfmapped = dfmapped.sort(dfmapped.col("BlockId").asc()); dfmapped.show(false);
		 

		// Groups the columns according the BlockId and creates a list of lists
		// with the EntityId and Number of associated blocks
		
		 Dataset<Row> dfreduced = dfmapped.groupBy("BlockId").agg(collect_list(dfmapped.col("entity,numberofBlocks")).alias("Entity_BlockNumberList")).sort(dfmapped.col("BlockId").asc());
		 dfreduced.show(false); 
		 dfreduced.printSchema();
		 

		// Uses the UDF createPairsUdf and extractInfoUdf and creates 2 columns
		// with the unique pairs in every block and the useful information for
		// the weight computation
		
		 dfreduced = dfreduced.withColumn("PairsList", callUDF("createPairs",col("Entity_BlockNumberList"))).withColumn("InfoList", callUDF("extractInfo", col("Entity_BlockNumberList")));
		 dfreduced.show(false);
		 

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
		Dataset<Row> dfnodesCEP = dfnodes;

		// ========================================= Weighted Node Pruning
		System.out.println("WNP");
		
		// Creates a column with all information nested in an array to explode it so we have every node in a single column for later group by.
		Dataset<Row> dfnodesWNP = dfnodes
				.withColumn("AllTogether",
						array(struct(dfnodes.col("node").getItem(0),
								struct(dfnodes.col("node").getItem(1), dfnodes.col("JaccardWeight"))),
								struct(dfnodes.col("node").getItem(1),
										struct(dfnodes.col("node").getItem(0), dfnodes.col("JaccardWeight")))))
				.drop("node").drop("JaccardWeight");
		dfnodesWNP.show(false);
		dfnodesWNP = dfnodesWNP.withColumn("AllTogether", explode(dfnodesWNP.col("AllTogether")));
		
		// Extracts the 1st node from every pair. We have the (i, j.Wij) (j, i.Wij) in a single column
		dfnodesWNP = dfnodesWNP.withColumn("Node", dfnodesWNP.col("AllTogether").getItem("col1"))
							   .withColumn("Node2_Weight", dfnodesWNP.col("AllTogether").getItem("col2")).drop("AllTogether");
		Dataset<Row> dfnodesCNP = dfnodesWNP;
		
		// Groups by EntityID and collects a list with the neighborhood in order to calculate the average Weight for every neighborhood
		dfnodesWNP = dfnodesWNP.groupBy("Node").agg(collect_list(dfnodesWNP.col("Node2_Weight")).as("Node2_Weight")).sort(dfnodesWNP.col("Node").asc());
		
		// Creates a column with the size of the neighborhood and a list with the corresponding weight of every neighbor entity.
		dfnodesWNP = dfnodesWNP.withColumn("Size", size(dfnodesWNP.col("Node2_Weight")))
							   .withColumn("List", dfnodesWNP.col("Node2_Weight.JaccardWeight"));
		
		// Calling the averageWeight UDF which reads every list in a column and calculates the average of the list.
		// In the specific calculates the average weight of the neighborhood.
		dfnodesWNP = dfnodesWNP.withColumn("Average", callUDF("averageWeight", dfnodesWNP.col("List"), dfnodesWNP.col("Size")))
				.drop("Size").drop("List");
		
		// Exploding the list again to filter our data and prune whatever we don't need.
		dfnodesWNP = dfnodesWNP.withColumn("Node2_Weight", explode(dfnodesWNP.col("Node2_Weight")));
		
		dfnodesWNP = dfnodesWNP.select("Node", "Node2_Weight").filter(col("Node2_Weight.JaccardWeight").gt(dfnodesWNP.col("Average")));
		
		System.out.println("====================================================WNP RESULT====================================================");
		dfnodesWNP = dfnodesWNP.withColumn("node", array(dfnodesWNP.col("node"), dfnodesWNP.col("Node2_Weight.col1"))).withColumn("JaccardWeight", dfnodesWNP.col("Node2_Weight.JaccardWeight")).drop("Node2_Weight");
		dfnodesWNP.show(false);
		dfnodesWNP.printSchema();
		System.out.println("==================================================================================================================");


		// ========================================= Weighted Edge Pruning
		double meanWeight = dfnodes.select(avg(dfnodes.col("JaccardWeight"))).head().getDouble(0);
		// MPHKE SE SXOLIA GIA NA TO VGALW META TO XREIAZOMAI!
		System.out.println("====================================================WEP RESULT====================================================");
		dfnodes.select("node","JaccardWeight").filter(col("JaccardWeight").gt(meanWeight)).show(false);
		System.out.println("==================================================================================================================");
		
		
		// ========================================= Cardinality Node Pruning
		dfnodesCNP = dfnodesCNP.orderBy(desc("Node2_Weight.JaccardWeight")).groupBy("Node").agg(collect_list(dfnodesCNP.col("Node2_Weight")).as("Node2_Weight"));
		dfnodesCNP.show(false);
		dfnodesCNP = dfnodesCNP.withColumn("Node2_Weight_Filtered",slice(dfnodesCNP.col("Node2_Weight"),1,1)).drop("Node2_Weight"); // second argument is the N 
		dfnodesCNP = dfnodesCNP.withColumn("Node2_Weight_Filtered", explode(dfnodesCNP.col("Node2_Weight_Filtered")));
		System.out.println("====================================================CNP RESULT====================================================");
		dfnodesCNP = dfnodesCNP.withColumn("Node", sort_array(array(dfnodesCNP.col("Node"), dfnodesCNP.col("Node2_Weight_Filtered.col1")))).withColumn("JaccardWeight", dfnodesCNP.col("Node2_Weight_Filtered.JaccardWeight")).drop("Node2_Weight_Filtered");
		dfnodesCNP.show(false);
		System.out.println("==================================================================================================================");
		
		
		// ========================================== Cardinality Edge Pruning
		Dataset<Row> dfnodesCEPcount = dfnodesCEP.groupBy("JaccardWeight").count().sort(desc("JaccardWeight"));
		
		dfnodesCEPcount = dfnodesCEPcount.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id())));
		dfnodesCEPcount = dfnodesCEPcount.filter(col("id").equalTo(3)).select("JaccardWeight");
		double valueOfMinWeight = dfnodesCEPcount.head().getDouble(0);
		
		
		System.out.println("====================================================CEP RESULT====================================================");
		dfnodesCEP = dfnodesCEP.filter(col("JaccardWeight").geq(valueOfMinWeight)).select("node", "JaccardWeight");
		dfnodesCEP.show(false);
		System.out.println("==================================================================================================================");
		
	}

}