package comparisonBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.array_sort;
import static org.apache.spark.sql.functions.array_intersect;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.callUDF;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;

import edgeBasedStrategy.createPairsUdf;

public class ComparisonBasedWNP {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().appName("Comparison Based Strategy").config("spark.master", "local")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		spark.udf().register("CommonBlocksUdfWNP", new CalculateCommonBlocksUDF(),
				DataTypes.createArrayType(DataTypes.LongType));

		spark.udf().register("BlockSizeUdfWNP", new CalculateBlockSizeUDF(),
				DataTypes.createArrayType(DataTypes.LongType));

		spark.udf().register("GetWeightUdfWNPJaccard", new GetWeightUDFJaccard(),
				DataTypes.createArrayType(DataTypes.DoubleType));
		spark.udf().register("GetWeightUdfWNPCBS", new GetWeightUDFCBS(),
				DataTypes.createArrayType(DataTypes.DoubleType));
		spark.udf().register("GetWeightUdfWNPECBS", new GetWeightUDFECBS(),
				DataTypes.createArrayType(DataTypes.DoubleType));

		spark.udf().register("CreateNodePairs", new createPairsUdf(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));

		Dataset<Row> df = spark.read().json("data/blocks.json");
		Dataset<Row> dfAmazon = spark.read().format("csv").option("header", "true")
				.load("data/AmazonGoogle/Amazon.csv");
		Dataset<Row> dfGoogle = spark.read().format("csv").option("header", "true")
				.load("data/AmazonGoogle/Google.csv");
		Dataset<Row> dfUnion = dfAmazon.union(dfGoogle);

		Dataset<Row> dfBlocked = blocking(dfUnion);
		Dataset<Row> dfmapped = blockFiltering(df);
//		dfmapped.cache();
		Dataset<Row> dfPreprocessing = preprocessing(dfmapped);
		dfmapped.cache();
		Dataset<Row> dfWNP = wnpPruning(dfPreprocessing);
		dfWNP.sort(dfWNP.col("Weight").desc()).show(false);
		dfWNP.printSchema();
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
//		df.show(false);
		df.printSchema();
		df.cache();
		return df;
	}

	private static Dataset<Row> blockFiltering(Dataset<Row> df) {
		/*
		 * Calculates the cardinality of each block and creates a new column to save the
		 * data
		 */
		df = df.withColumn("ofEntities", size(df.col("entities")))
				.withColumn("cardinality", expr("int(((ofEntities-1)*ofEntities)/2)")).drop("ofEntities");
		/*
		 * Sorting the columns according to cardinality and breaks the arrays of
		 * entities
		 */
		df = df.sort(df.col("cardinality").asc()).withColumn("entityId", explode(df.col("entities"))).drop("entities");
		/*
		 * Groups data by entityID to get rid of duplicates and collect in a list the
		 * blocks that are associated with
		 */
		df = df.groupBy("entityId").agg(collect_list(df.col("block")).alias("AssociatedBlocks"))
				.sort(df.col("entityId").asc());
		return df;
	}

	private static Dataset<Row> preprocessing(Dataset<Row> df) {
		df = df.withColumn("AssociatedBlocks", array_sort(df.col("AssociatedBlocks")));
		// dfPreprocessing.show(false);

		df = df.withColumn("BlockId", explode(df.col("AssociatedBlocks")))
				.withColumn("EntityId_AssociatedBlocks", struct(df.col("entityId"), df.col("AssociatedBlocks")))
				.drop("entityId").drop("AssociatedBlocks").sort("BlockId");
		// df.show(false);
		df = df.groupBy("BlockId").agg(collect_list("EntityId_AssociatedBlocks").as("EntityId_AssociatedBlocks"));
		df = df.withColumn("listSize", size(df.col("EntityId_AssociatedBlocks")));
		df = df.filter(col("listSize").geq(2)).select("BlockId", "EntityId_AssociatedBlocks");
//		df.show(false);
		return df;
	}

	private static Dataset<Row> wnpPruning(Dataset<Row> df) {

		int scheme = 1;
		int BlockSize = 0;
		if (scheme == 1) {
//			df = df.withColumn("Cardinality", expr("int(((size(EntityId_AssociatedBlocks)-1)*size(EntityId_AssociatedBlocks))/2)"));
			df.show(false);
			df = df.withColumn("EntityIdList", df.col("EntityId_AssociatedBlocks.entityId"));
			df = df.withColumn("Edges", callUDF("CreateNodePairs", df.col("EntityIdList"))).drop("EntityIdList");
			df.cache();
//			df.show(false);
			df = df.withColumn("Cardinality",size(df.col("Edges")));
//			df.cache();
//			df.show(false);
			df = df.drop("EntityId_AssociatedBlocks").withColumn("Edges2", explode(df.col("Edges"))).drop("Edges");
//			df.show(false);
			df = df.groupBy("Edges2").agg(sum(expr("1/Cardinality")).as("Weight"),
					collect_list("BlockId").as("BlockId"));
//			df.show(false);
			df = df.withColumn("BlockId", explode(df.col("BlockId")));
//			df.show(false);

//			df.show(false);
//			df.cache();
			df = df.withColumnRenamed("Edges2", "Edges");
//			df = df.groupBy("BlockId").agg(collect_list(df.col("Edges2")).as("Edges"), collect_list("Weight").as("WeightsList"));
			df.show(false);
		} else {
			if(scheme == 4) {
				BlockSize = (int) df.count();
			}
				

			df = df.withColumn("EntityIdList", df.col("EntityId_AssociatedBlocks.entityId"));
//			df.show(false);

			df = df.withColumn("Edges", callUDF("CreateNodePairs", df.col("EntityIdList"))).drop("EntityIdList");
			df.show(false);
//			df.cache();
			df = df.withColumn("CommonBlocks",
					callUDF("CommonBlocksUdfWNP", df.col("EntityId_AssociatedBlocks.AssociatedBlocks")))
					.withColumn("BlockSize",
							callUDF("BlockSizeUdfWNP", df.col("EntityId_AssociatedBlocks.AssociatedBlocks")));
			df.show(false);
			df.cache();
//			df.show(false);

			switch (scheme) {
			case 1:
//				ARCSScheme();
				break;
			case 2:
				df = JaccardScheme(df);
				break;
			case 3:
				df = CBSScheme(df);
				break;
			case 4:
				df = ECBSScheme(df, BlockSize);
				break;
			}
			String transform_expr = "transform(WeightsList, (x, i) -> struct(x as element1, Edges[i] as element2))";
			df = df.withColumn("merged_arrays", explode(expr(transform_expr)))
					.withColumn("Weight", col("merged_arrays.element1"))
					.withColumn("Edges", col("merged_arrays.element2")).drop("merged_arrays").drop("WeightsList");
		}

//		df = df.withColumn("WeightsList", callUDF("GetWeightUdfWNPJaccard", df.col("BlockSize"), df.col("CommonBlocks")))
//				.drop("EntityId_AssociatedBlocks").drop("CommonBlocks").drop("BlockSize").drop("BlockId");
//		df.show(false);

		df = df.dropDuplicates();
		df.cache();

		df = df.withColumn("test", array(df.col("Edges").getItem(1), df.col("Edges").getItem(0)));
		df = df.withColumn("BothEdges", array(df.col("Edges"), df.col("test"))).drop("test").drop("Edges");
		df = df.withColumn("Edges", explode(df.col("BothEdges"))).drop("BothEdges");
//		df.cache();
		df = df.withColumn("Node2_Weight", struct(df.col("Edges").getItem(1), df.col("Weight")))
				.withColumn("Node", df.col("Edges").getItem(0)).drop("JaccardWeight").select("Node", "Node2_Weight");

		df = df.groupBy("Node").agg(collect_list(df.col("Node2_Weight")).as("Node2_Weight"),
				expr("avg(Node2_Weight.Weight)").as("Average"));
		df = df.withColumn("Node2_Weight", explode(df.col("Node2_Weight")));
		df = df.select("Node", "Node2_Weight").filter(col("Node2_Weight.Weight").gt(df.col("Average")));
		df = df.withColumn("Edge", array_sort(array(df.col("Node"), df.col("Node2_Weight.col1"))))
				.withColumn("Weight", df.col("Node2_Weight.Weight")).drop("Node2_Weight").drop("Node");
		df = df.dropDuplicates();

		return df;
	}

	private static Dataset<Row> JaccardScheme(Dataset<Row> df) {
		df = df.withColumn("WeightsList",
				callUDF("GetWeightUdfWNPJaccard", df.col("BlockSize"), df.col("CommonBlocks")))
				.drop("EntityId_AssociatedBlocks").drop("CommonBlocks").drop("BlockSize").drop("BlockId");
		df.show(false);
		return df;
	}

	private static Dataset<Row> CBSScheme(Dataset<Row> df) {
		df = df.withColumn("WeightsList", callUDF("GetWeightUdfWNPCBS", df.col("BlockSize"), df.col("CommonBlocks")))
				.drop("EntityId_AssociatedBlocks").drop("CommonBlocks").drop("BlockSize").drop("BlockId");
		df.show(false);
		return df;
	}

	private static Dataset<Row> ECBSScheme(Dataset<Row> df, int NumberOfBlocks) {
		df = df.withColumn("NumberOfBlocks", lit(NumberOfBlocks));
		df = df.withColumn("WeightsList",
				callUDF("GetWeightUdfWNPECBS", df.col("BlockSize"), df.col("CommonBlocks"), df.col("NumberOfBlocks")))
				.drop("EntityId_AssociatedBlocks").drop("CommonBlocks").drop("BlockSize").drop("BlockId");
		df.show(false);
		return df;
	}
}
