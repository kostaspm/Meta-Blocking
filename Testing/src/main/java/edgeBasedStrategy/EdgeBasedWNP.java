package edgeBasedStrategy;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

public class EdgeBasedWNP {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Edge Based Strategy").config("spark.master", "local")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		Dataset<Row> df = spark.read().json("data/blocks.json");
		Dataset<Row> dfAmazon = spark.read().format("csv").option("header", "true")
				.load("data/AmazonGoogle/Amazon.csv");
		Dataset<Row> dfGoogle = spark.read().format("csv").option("header", "true")
				.load("data/AmazonGoogle/Google.csv");
		spark.udf().register("createPairs", new createPairsUdf(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
		spark.udf().register("extractInfo", new extractInfoUdf(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType)));
		spark.udf().register("averageWeight", new averageWeightUdf(), DataTypes.DoubleType);
		// df.show(false);
		Dataset<Row> dfUnion = dfAmazon.union(dfGoogle);
		Dataset<Row> dfBlocked = blocking(dfUnion);
		// Stage 1: Block Filtering
		Dataset<Row> dfmapped = blockFiltering(dfBlocked);

		// dfmapped.show(false);
		System.out.println(
				"==============================================================================================");
		// Stage 2: Preprocessing

		Dataset<Row> dfnodes = preprocessing(dfmapped);
		dfnodes.show(false);
		dfnodes.cache();
		// dfnodes.sort(dfnodes.col("JaccardWeight").desc()).show(false);

		// Stage 3 Pruning
		// Creates the prefered schema for the stage 3 (Pruning Stage)
		// [node1, node2] | Weight

		// ========================================= Weighted Node Pruning
		System.out.println("WNP");
		Dataset<Row> dfnodesWNP = wnpPruning(dfnodes);

		System.out.println(
				"====================================================WNP RESULT====================================================");
		dfnodesWNP = dfnodesWNP.withColumn("node", array(dfnodesWNP.col("node"), dfnodesWNP.col("Node2_Weight.col1")))
				.withColumn("JaccardWeight", dfnodesWNP.col("Node2_Weight.JaccardWeight")).drop("Node2_Weight");
		dfnodesWNP = dfnodesWNP.sort(dfnodesWNP.col("JaccardWeight").desc());
		dfnodesWNP.show(false);
		dfnodesWNP.printSchema();
		System.out.println(
				"==================================================================================================================");
	}

	private static Dataset<Row> blocking(Dataset<Row> df) {
		df = df.withColumn("title", lower(df.col("title")))
				.withColumn("entityid", row_number().over(Window.orderBy(lit(1)))).drop("description")
				.drop("manufacturer").drop("price").drop("id");
		df = df.withColumn("entityid", df.col("entityid").cast(DataTypes.LongType));
		df = df.withColumn("title", split(df.col("title"), " |\\,|\\(|\\)|\\&|\\:|\\/|\\-"));
		df = df.withColumn("titleTokens", explode(df.col("title"))).drop("title");
		df = df.groupBy("titletokens").agg(collect_list("entityid").as("entities"));
		df = df.withColumn("block", row_number().over(Window.orderBy(lit(1)))).drop("titletokens");
		df = df.withColumn("block", df.col("block").cast(DataTypes.LongType));
		df = df.select("block", "entities");
		df = df.withColumn("entities", array_distinct(df.col("entities")));
		df.show(false);
		df.printSchema();
		df.cache();
		return df;
	}

	private static Dataset<Row> blockFiltering(Dataset<Row> df) {
		/*
		 * Calculates the cardinality of each block and creates a new column to
		 * save the data
		 */
		df = df.withColumn("ofEntities", size(df.col("entities")))
				.withColumn("cardinality", expr("int(((ofEntities-1)*ofEntities)/2)")).drop("ofEntities");
		/*
		 * Sorting the columns according to cardinality and breaks the arrays of
		 * entities
		 */
		df = df.sort(df.col("cardinality").asc()).withColumn("entityId", explode(df.col("entities"))).drop("entities");
		/*
		 * Groups data by entityID to get rid of duplicates and collect in a
		 * list the blocks that are associated with
		 */
		df = df.groupBy("entityId").agg(collect_list(df.col("block")).alias("AssociatedBlocks"))
				.sort(df.col("entityId").asc());
		return df;
	}

	private static Dataset<Row> preprocessing(Dataset<Row> df) {

		// switch(selectedScheme){
		// case 1:
		// //preprocessingARCS();
		// break;
		// case 2:
		// //preprocessingCBS();
		// break;
		// case 3:
		// //preprocessingECBS();
		// break;
		// case 4:
		// preprocessingJS();
		// break;
		// case 5:
		// //reprocessingEJS();
		// break;
		// }

		// ==================================================== Map 1
		// Counts the number of blocks in each Entity and breaks the list of
		// blocks
		df = df.withColumn("NumberOfBlocks", size(df.col("AssociatedBlocks"))).withColumn("AssociatedBlocks",
				explode(df.col("AssociatedBlocks")));

		// df.show(false);

		// Creates an 1x2 array with entity and number of associated blocks (we
		// need that later too)

		df = df.withColumn("entity,numberofBlocks", struct(df.col("entityId"), df.col("NumberOfBlocks")))
				.drop("entityId").drop("NumberOfBlocks").withColumnRenamed("AssociatedBlocks", "BlockId");
		// df = df.sort(df.col("BlockId").asc());
		// df.show(false);
		// df.printSchema();

		// Groups the columns according the BlockId and creates a list of lists
		// with the EntityId and Number of associated blocks

		df = df.groupBy("BlockId").agg(collect_list(df.col("entity,numberofBlocks")).alias("Entity_BlockNumberList"))
				.sort(df.col("BlockId").asc());
		// dfreduced.show(false);
		// dfreduced.printSchema();

		// ==================================================== Reduce 1
		// Uses the UDF createPairsUdf and extractInfoUdf and creates 2 columns
		// with the unique pairs in every block and the useful information for
		// the weight computation

		df = df.withColumn("PairsList", callUDF("createPairs", col("Entity_BlockNumberList.entityId"))).drop("BlockId");
		// df1.show(false);

		Dataset<Row> df1Test = df.withColumn("PairsList", explode(df.col("PairsList")));

		df = df.withColumn("NumberOfBlocks1_NumberOfBlocks2",
				callUDF("extractInfo", col("Entity_BlockNumberList.NumberOfBlocks"))).drop("Entity_BlockNumberList");
		// df1.show(false);
		// df1.printSchema();

		String transform_expr = "transform(PairsList, (x, i) -> struct(x as element1, NumberOfBlocks1_NumberOfBlocks2[i] as element2))";
		df = df.withColumn("merged_arrays", explode(expr(transform_expr)))
				.withColumn("Edges", col("merged_arrays.element1"))
				.withColumn("NumberOfBlocks", col("merged_arrays.element2")).drop("merged_arrays").drop("PairsList")
				.drop("NumberOfBlocks1_NumberOfBlocks2");

		// df1.show(false);
		df = df.dropDuplicates("Edges", "NumberOfBlocks");
		// df1.show(false);

		// Counts how many times there is a pair among all blocks.
		// So it is the commong blocks between two entities
		System.out.println("Common blocks");
		df1Test = df1Test.groupBy("PairsList").count();
		// df1Test.show(false);

		df1Test = df1Test.join(df, df1Test.col("PairsList").equalTo(df.col("Edges")));
		df1Test = df1Test.withColumn("iEntityNumberOfBlocks", col("NumberOfBlocks").getItem(0))
				.withColumn("jEntityNumberOfBlocks", col("NumberOfBlocks").getItem(1)).drop("NumberOfBlocks")
				.drop("PairsList");
		df1Test = df1Test.select(col("Edges").as("Node"), col("iEntityNumberOfBlocks"), col("jEntityNumberOfBlocks"),
				col("count").as("CommonBlocks"));

		// df1Test.show(false);

		// I need to find how to combine with other information

		// Job 2 Weight Calculation
		Dataset<Row> dfnodes = df1Test
				.withColumn("JaccardWeight",
						expr("double(CommonBlocks) / (double(iEntityNumberOfBlocks) + double(jEntityNumberOfBlocks) - double(CommonBlocks))"))
				.drop("iEntityNumberOfBlocks").drop("jEntityNumberOfBlocks").drop("CommonBlocks");
		// dfnodes.cache();
		// dfnodes.show(false);
		return dfnodes;
	}

	private static Dataset<Row> wnpPruning(Dataset<Row> df) {

		df = df.withColumn("test", array(df.col("Node").getItem(1), df.col("Node").getItem(0)));
		df = df.withColumn("BothEdges", array(df.col("Node"), df.col("test"))).drop("test").drop("Node");
		df = df.withColumn("Node", explode(df.col("BothEdges"))).drop("BothEdges");
		df = df.withColumn("Node2_Weight", struct(df.col("Node").getItem(1), df.col("JaccardWeight")))
				.withColumn("Node", df.col("Node").getItem(0)).drop("JaccardWeight").select("Node", "Node2_Weight");
		df.show(false);
		
		df = df.groupBy("Node").agg(collect_list(df.col("Node2_Weight")).as("Node2_Weight"),
				expr("avg(Node2_Weight.JaccardWeight)").as("Average"));
//		// Exploding the list again to filter our data and prune whatever we
//		// don't need.
		df = df.withColumn("Node2_Weight", explode(df.col("Node2_Weight")));
		df.show(false);
		df = df.select("Node", "Node2_Weight").filter(col("Node2_Weight.JaccardWeight").gt(df.col("Average")));
		return df;
	}
}