package entityBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.array_remove;
import static org.apache.spark.sql.functions.array_sort;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.array_max;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.sum;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

public class EntityBasedWEP {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Comparison Based Strategy").config("spark.master", "local")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		spark.udf().register("getFrequencies", new getFrequencies(), DataTypes.createArrayType(DataTypes.IntegerType));
		spark.udf().register("getFrequencies", new GetFrequenciesUDF(),
				DataTypes.createArrayType(DataTypes.IntegerType));
		spark.udf().register("createPairs", new CreatePairsUdfEntityBased(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
		spark.udf().register("filterNodes", new FilterNodesUdfARCS(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)));

		spark.udf().register("getTotalWeightWEPJaccard", new GetTotalWeightWEPJaccard(), DataTypes.DoubleType);
		spark.udf().register("getTotalWeightWEPCBS", new GetTotalWeightWEPCBS(), DataTypes.DoubleType);
		spark.udf().register("getTotalWeightWEPECBS", new GetTotalWeightWEPECBS(), DataTypes.DoubleType);

		spark.udf().register("getNumberOfEdgesWEP", new GetNumberOfEdgesWEP(), DataTypes.LongType);

		spark.udf().register("getWeightListWEPJaccard", new GetWeightListWEPJaccard(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)));
		spark.udf().register("getWeightListWEPCBS", new GetWeightListWEPCBS(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)));
		spark.udf().register("getWeightListWEPECBS", new GetWeightListWEPECBS(),
				DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)));

		Dataset<Row> df = spark.read().json("data/blocks.json");
		Dataset<Row> dfAmazon = spark.read().format("csv").option("header", "true")
				.load("data/AmazonGoogle/Amazon.csv");
		Dataset<Row> dfGoogle = spark.read().format("csv").option("header", "true")
				.load("data/AmazonGoogle/Google.csv");
		Dataset<Row> dfUnion = dfAmazon.union(dfGoogle);

		Dataset<Row> dfBlocked = blocking(dfUnion);

		Dataset<Row> dfFiltered = blockFiltering(dfBlocked);

		Dataset<Row> dfPreprocessing = preprocessing(dfFiltered);

		Dataset<Row> dfPruned = wepPruning(dfPreprocessing, dfFiltered);
		dfPruned.sort(dfPruned.col("Weight").desc()).show(false);
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
		System.out.println(
				"==============================================================================================");

		// Stage 2 Preprocessing
		System.out.println("Start of Preprocessing ");
		df = df.withColumn("BlockId", explode(df.col("AssociatedBlocks"))).drop("AssociatedBlocks");
		df = df.groupBy("BlockId").agg(collect_list("entityId").as("EntityList"));
		df = df.withColumn("listSize", size(df.col("EntityList")));
		df = df.filter(col("listSize").geq(2)).drop("listSize");
		df.show(false);
		System.out.println("End of Preprocessing");

		System.out
				.println("==========================================================================================");
		return df;
	}

	private static Dataset<Row> wepPruning(Dataset<Row> df, Dataset<Row> dfTest) {
		int scheme = 1;
		if (scheme == 1) {
//			df.show(false);
			df = df.withColumn("Cardinality", expr("int(((size(EntityList)-1)*size(EntityList))/2)"));
//			df.show(false);
			df = df.withColumn("PairsList", callUDF("createPairs", df.col("EntityList")));
//			df.show(false);
			df = df.withColumn("PairsList", explode(df.col("PairsList")));
//			df.show(false);
			df = df.groupBy("PairsList").agg(sum(expr("1/Cardinality")).as("Weight"));
			df = df.dropDuplicates();
			int totalEdges = (int) df.count();
			double totalWeight = df.agg(sum(df.col("Weight"))).head().getDouble(0);
			df.show(false);
			df= df.withColumnRenamed("PairsList", "Edge");
			df = df.filter(df.col("Weight").gt(totalWeight/totalEdges));
			
//			df = df.withColumn("BlockId", explode(df.col("BlockId")));
//
//			df = df.withColumn("iEntityId", df.col("PairsList").getItem(0))
//					.withColumn("jEntityId", df.col("PairsList").getItem(1)).drop("PairsList");
//			df.show(false);
//			df = df.groupBy("BlockId").agg(collect_list("iEntityId").as("iEntityId"),
//					collect_list("jEntityId").as("jEntityId"), collect_list("Weight").as("Weight"),
//					sum("Weight").as("NeighWeight"));
//			df.show(false);
//			df = df.withColumn("MeanWeight", expr("NeighWeight/ size(Weight)"));
//			df.show(false);
//			df = df.withColumn("Filtered",
//					callUDF("filterNodes", df.col("iEntityId"), df.col("jEntityId"), df.col("Weight"),
//							df.col("MeanWeight")))
//					.drop("BlockId").drop("jEntityId").drop("iEntityId").drop("Weight").drop("NeighWeight")
//					.drop("MeanWeight");
//			df.show(false);
//			df = df.withColumn("Filtered", explode(df.col("Filtered")));
//			df = df.withColumn("Edge",
//					array(df.col("Filtered").getItem(0).cast(DataTypes.LongType),
//							df.col("Filtered").getItem(1).cast(DataTypes.LongType)))
//					.withColumn("Weight", df.col("Filtered").getItem(2).cast(DataTypes.DoubleType)).drop("Filtered");
			df.show(false);
			return df;
		}
		df = df.withColumn("EntityId", explode(df.col("EntityList"))).drop("BlockId");
		df = df.select("EntityId", "EntityList");
		df = df.withColumnRenamed("EntityList", "CoOccurrenceBag");

		df = df.groupBy("EntityId").agg(flatten(collect_list("CoOccurrenceBag")).as("CoOccurrenceBag"));

		df = df.withColumn("maxElement", array_max(df.col("CoOccurrenceBag")));
		long maxelement = (long) df.select("maxElement").sort(df.col("maxElement").desc()).head().get(0);
		df = df.withColumn("maxElement", lit((int) maxelement));
//		df.show(false);
		// The array frequencies contains for eve
		df = df.withColumn("Frequencies",
				callUDF("getFrequencies", df.col("CoOccurrenceBag"), df.col("maxElement"))
						.cast(DataTypes.createArrayType(DataTypes.LongType)))
				.withColumn("SetOfNeighbors", array_distinct(df.col("CoOccurrenceBag"))).drop("maxElement");
		df = df.withColumn("SetOfNeighborsWithoutID", array_remove(df.col("SetOfNeighbors"), df.col("EntityId")));
		df = df.sort("EntityId");
//		df.show(false);
		switch (scheme) {
		case 1:
//			ARCSScheme();
			break;
		case 2:
			df = JaccardScheme(df, dfTest);
			break;
		case 3:
			df = CBSScheme(df);
			break;
		case 4:
			df = ECBSScheme(df, dfTest);
			break;
		}

		return df;
	}

	private static Dataset<Row> JaccardScheme(Dataset<Row> df, Dataset<Row> dfTest) {
		dfTest = dfTest.withColumn("arraysize", size(dfTest.col("AssociatedBlocks"))).withColumnRenamed("entityId",
				"ent2");

		// Prepei na dior8wsw auto to kommati
		// =============================================================
		List<Row> test = dfTest.select("arraysize").collectAsList();

		ArrayList<Integer> testlist = new ArrayList<Integer>();
		test.forEach(m -> testlist.add((Integer) m.get(0)));
		dfTest.show(false);

		String str = testlist.toString();
		str = str.substring(1, str.length() - 1);
		System.out.println(str);

		df = df.withColumn("NumberOfBlocks", split(lit(str), ", ").cast(DataTypes.createArrayType(DataTypes.LongType)));
		// ================================================================================================

		df = df.withColumn("Weight",
				callUDF("getTotalWeightWEPJaccard", df.col("Frequencies"), df.col("SetOfNeighborsWithoutID"),
						df.col("NumberOfBlocks"), df.col("EntityId")))
				.withColumn("NumberOfEdges", callUDF("GetNumberOfEdgesWEP", df.col("Frequencies"),
						df.col("SetOfNeighborsWithoutID"), df.col("EntityId")));
		double totalWeight = df.agg(sum(df.col("Weight"))).first().getDouble(0);
		long totaledges = df.agg(sum(df.col("NumberOfEdges"))).first().getLong(0);
		df.printSchema();
		df.show(false);
		df = df.withColumn("WeightList",
				callUDF("GetWeightListWEPJaccard", df.col("Frequencies"), df.col("SetOfNeighborsWithoutID"),
						df.col("NumberOfBlocks"), df.col("EntityId"), df.col("Weight"), df.col("NumberOfEdges")))
				.drop("CoOccurrenceBag").drop("Frequencies").drop("SetOfNeighbors").drop("SetOfNeighborsWithoutID")
				.drop("NumberOfBlocks").drop("Weight").drop("NumberOfEdges");
		df.show(false);
		df = df.filter(size(col("WeightList")).gt(0)).select("EntityId", "WeightList");
		df = df.withColumn("jEntity_Weight", explode(df.col("WeightList"))).drop("WeightList");
		df = df.withColumn("Edge",
				array(df.col("EntityId"), df.col("jEntity_Weight").getItem(0).cast(DataTypes.LongType)))
				.withColumn("Weight", df.col("jEntity_Weight").getItem(1)).drop("jEntity_Weight").drop("EntityId");
		df = df.withColumn("Edge", array_sort(df.col("Edge")));
		df = df.filter(col("Weight").gt(totalWeight / totaledges));
		df = df.dropDuplicates();
		df = df.select("Edge", "Weight");
		return df;
	}

	private static Dataset<Row> CBSScheme(Dataset<Row> df) {
		df = df.withColumn("Weight",
				callUDF("getTotalWeightWEPCBS", df.col("Frequencies"), df.col("SetOfNeighborsWithoutID"),
						df.col("EntityId")))
				.withColumn("NumberOfEdges", callUDF("GetNumberOfEdgesWEP", df.col("Frequencies"),
						df.col("SetOfNeighborsWithoutID"), df.col("EntityId")));
		double totalWeight = df.agg(sum(df.col("Weight"))).first().getDouble(0);
		long totaledges = df.agg(sum(df.col("NumberOfEdges"))).first().getLong(0);
		df.printSchema();
		df.show(false);
		df = df.withColumn("WeightList",
				callUDF("GetWeightListWEPCBS", df.col("Frequencies"), df.col("SetOfNeighborsWithoutID"),
						df.col("EntityId"), df.col("Weight"), df.col("NumberOfEdges")))
				.drop("CoOccurrenceBag").drop("Frequencies").drop("SetOfNeighbors").drop("SetOfNeighborsWithoutID")
				.drop("Weight").drop("NumberOfEdges");
		df.show(false);
		df = df.filter(size(col("WeightList")).gt(0)).select("EntityId", "WeightList");
		df = df.withColumn("jEntity_Weight", explode(df.col("WeightList"))).drop("WeightList");
		df = df.withColumn("Edge",
				array(df.col("EntityId"), df.col("jEntity_Weight").getItem(0).cast(DataTypes.LongType)))
				.withColumn("Weight", df.col("jEntity_Weight").getItem(1)).drop("jEntity_Weight").drop("EntityId");
		df = df.withColumn("Edge", array_sort(df.col("Edge")));
		df = df.filter(col("Weight").gt(totalWeight / totaledges));
		df = df.dropDuplicates();
		df = df.select("Edge", "Weight");

		return df;
	}

	private static Dataset<Row> ECBSScheme(Dataset<Row> df, Dataset<Row> dfTest) {
		int BlockSize = (int) dfTest.count();
		dfTest = dfTest.withColumn("arraysize", size(dfTest.col("AssociatedBlocks"))).withColumnRenamed("entityId",
				"ent2");

		// Prepei na dior8wsw auto to kommati
		// =============================================================
		List<Row> test = dfTest.select("arraysize").collectAsList();

		ArrayList<Integer> testlist = new ArrayList<Integer>();
		test.forEach(m -> testlist.add((Integer) m.get(0)));
		dfTest.show(false);

		String str = testlist.toString();
		str = str.substring(1, str.length() - 1);
		System.out.println(str);

		df = df.withColumn("NumberOfBlocks", split(lit(str), ", ").cast(DataTypes.createArrayType(DataTypes.LongType)));
		// ================================================================================================
		df = df.withColumn("BlockSize", lit(BlockSize));

		df = df.withColumn("Weight",
				callUDF("getTotalWeightWEPECBS", df.col("Frequencies"), df.col("SetOfNeighborsWithoutID"),
						df.col("NumberOfBlocks"), df.col("EntityId"), df.col("BlockSize")))
				.withColumn("NumberOfEdges", callUDF("GetNumberOfEdgesWEP", df.col("Frequencies"),
						df.col("SetOfNeighborsWithoutID"), df.col("EntityId")));

		double totalWeight = df.agg(sum(df.col("Weight"))).first().getDouble(0);
		long totaledges = df.agg(sum(df.col("NumberOfEdges"))).first().getLong(0);
		df.printSchema();
		df.show(false);
		df = df.withColumn("WeightList",
				callUDF("GetWeightListWEPECBS", df.col("Frequencies"), df.col("SetOfNeighborsWithoutID"),
						df.col("NumberOfBlocks"), df.col("EntityId"), df.col("Weight"), df.col("NumberOfEdges"),
						df.col("BlockSize")))
				.drop("CoOccurrenceBag").drop("Frequencies").drop("SetOfNeighbors").drop("SetOfNeighborsWithoutID")
				.drop("NumberOfBlocks").drop("Weight").drop("NumberOfEdges");
		df.show(false);
		df = df.filter(size(col("WeightList")).gt(0)).select("EntityId", "WeightList");
		df = df.withColumn("jEntity_Weight", explode(df.col("WeightList"))).drop("WeightList");
		df = df.withColumn("Edge",
				array(df.col("EntityId"), df.col("jEntity_Weight").getItem(0).cast(DataTypes.LongType)))
				.withColumn("Weight", df.col("jEntity_Weight").getItem(1)).drop("jEntity_Weight").drop("EntityId");
		df = df.withColumn("Edge", array_sort(df.col("Edge")));
		df = df.filter(col("Weight").gt(totalWeight / totaledges));
		df = df.dropDuplicates();
		df = df.select("Edge", "Weight");
		return df;
	}
}