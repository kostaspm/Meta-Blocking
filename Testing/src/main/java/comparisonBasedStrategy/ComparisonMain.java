package comparisonBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.array_sort;
import static org.apache.spark.sql.functions.array_intersect;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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
		dfPreprocessing = dfPreprocessing.groupBy("BlockId").agg(collect_list("EntityId_AssociatedBlocks").as("list"));
		dfPreprocessing = dfPreprocessing.withColumn("listSize", size(dfPreprocessing.col("list")));
		dfPreprocessing = dfPreprocessing.filter(col("listSize").geq(2)).select("BlockId", "list");
		System.out.println("After Preprocessing");
		dfPreprocessing.show(false);
		dfPreprocessing.printSchema();
		System.out.println(
				"==============================================================================================");
		
		// Stage 3 Pruning
		// Weighted Node Pruning
		Dataset<Row> dfWNP = dfPreprocessing.withColumn("Struct1", dfPreprocessing.col("list").getItem(0)).withColumn("Struct2", dfPreprocessing.col("list").getItem(1));
		dfWNP.printSchema();
	
		dfWNP = dfWNP.withColumn("Block1", dfWNP.col("Struct1.AssociatedBlocks")).withColumn("Block2", dfWNP.col("Struct2.AssociatedBlocks")).drop("Struct1").drop("Struct2");
		dfWNP = dfWNP.withColumn("JaccardWeight", expr("int(size(array_intersect(Block1, Block2)))/int(size(Block1) + size(Block2) - int(size(array_intersect(Block1, Block2))))"));
		dfWNP.show(false);
		
	}
}