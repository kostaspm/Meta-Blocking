package comparisonBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.array_sort;
import static org.apache.spark.sql.functions.array_intersect;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.callUDF;


import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import edgeBasedStrategy.createPairsUdf;

public class ComparisonMain {

	public static void main(String[] args) {
		
		SparkSession spark = SparkSession.builder().appName("Comparison Based Strategy").config("spark.master", "local")
				.getOrCreate();
		
		spark.sparkContext().setLogLevel("ERROR");
		spark.udf().register("CommonBlocksUdfWNP", new CalculateCommonBlocksUDF(), DataTypes.createArrayType(DataTypes.LongType));
		spark.udf().register("BlockSizeUdfWNP", new CalculateBlockSizeUDF(), DataTypes.createArrayType(DataTypes.LongType));
		spark.udf().register("GetWeightUdfWNP", new GetWeightWNPUDF(), DataTypes.createArrayType(DataTypes.DoubleType));
		spark.udf().register("CreateNodePairs", new createPairsUdf(), DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
		
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
		
		Dataset<Row> dfWNP = dfPreprocessing.withColumn("CommonBlocks", callUDF("CommonBlocksUdfWNP", dfPreprocessing.col("EntityId_AssociatedBlocks.AssociatedBlocks")))
				.withColumn("BlockSize", callUDF("BlockSizeUdfWNP", dfPreprocessing.col("EntityId_AssociatedBlocks.AssociatedBlocks")));
		dfWNP.show(false);
		
		dfWNP = dfWNP.withColumn("EntityIdList", dfWNP.col("EntityId_AssociatedBlocks.entityId"));
		dfWNP.show(false);
		dfWNP = dfWNP.withColumn("Edges", callUDF("CreateNodePairs", dfWNP.col("EntityIdList"))).drop("EntityIdList");
		dfWNP.show(false);
		
		//dfWNP = dfWNP.withColumn("Edges", col)
		
		dfWNP = dfWNP.withColumn("WeightsList", callUDF("GetWeightUdfWNP",dfWNP.col("BlockSize"), dfWNP.col("CommonBlocks"))).drop("EntityId_AssociatedBlocks").drop("CommonBlocks").drop("BlockSize").drop("BlockId");
		dfWNP.show(false);
		
		
		String transform_expr = "transform(WeightsList, (x, i) -> struct(x as element1, Edges[i] as element2))";
		dfWNP = dfWNP.withColumn("merged_arrays", explode(expr(transform_expr)))
				.withColumn("Weight", col("merged_arrays.element1"))
				.withColumn("Edges", col("merged_arrays.element2")).drop("merged_arrays")
				.drop("WeightsList");
		dfWNP.show(false);
		dfWNP = dfWNP.dropDuplicates(); //removing 
		//dfWNP = dfWNP.withColumn("entities", explode(dfWNP.col("EdgesNew")));

		dfWNP.show(false);
		
		
		/*
		 * // Stage 3 Pruning
		// Weighted Node Pruning
		
		Dataset<Row> dfWNP = dfPreprocessing.withColumn("EntityIdList", dfPreprocessing.col("EntityId_AssociatedBlocks.entityId"));
	
		dfWNP = dfWNP.withColumn("Edges", callUDF("CreateNodePairs", dfWNP.col("EntityIdList"))).drop("EntityIdList");
		
		dfWNP = dfWNP.withColumn("Edges", explode(dfWNP.col("Edges")));
		dfWNP = dfWNP.dropDuplicates("Edges");
		dfWNP = dfWNP.groupBy("BlockId").agg(collect_list("Edges").as("Edges"), first(col("EntityId_AssociatedBlocks")).as("EntityId_AssociatedBlocks"));
		dfWNP.show(false);
		dfWNP.printSchema();
		
		dfWNP = dfWNP.withColumn("CommonBlocks", callUDF("CommonBlocksUdfWNP", dfWNP.col("EntityId_AssociatedBlocks.AssociatedBlocks")))
				.withColumn("BlockSize", callUDF("BlockSizeUdfWNP", dfWNP.col("EntityId_AssociatedBlocks.AssociatedBlocks")));
		dfWNP.show(false);
		
		
		dfWNP = dfWNP.withColumn("WeightsList", callUDF("GetWeightUdfWNP",dfWNP.col("BlockSize"), dfWNP.col("CommonBlocks"))).drop("EntityId_AssociatedBlocks").drop("CommonBlocks").drop("BlockSize").drop("BlockId");
		dfWNP.show(false);
		
		
		String transform_expr = "transform(WeightsList, (x, i) -> struct(x as element1, Edges[i] as element2))";
		dfWNP = dfWNP.withColumn("merged_arrays", explode(expr(transform_expr)))
				.withColumn("Weight", col("merged_arrays.element1"))
				.withColumn("Edges", col("merged_arrays.element2")).drop("merged_arrays")
				.drop("WeightsList");
		dfWNP.show(false);
		dfWNP = dfWNP.withColumn("entities", explode(dfWNP.col("Edges")));

		dfWNP.show(false);
		*/
		
	}
}


