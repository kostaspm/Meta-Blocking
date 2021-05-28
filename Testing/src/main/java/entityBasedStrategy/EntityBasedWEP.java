package entityBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.array_remove;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.split;

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
		spark.udf().register("getTotalWeightWEP", new GetTotalWeightWEP(), DataTypes.DoubleType);
		spark.udf().register("getNumberOfEdgesWEP", new GetNumberOfEdgesWEP(), DataTypes.LongType);
		spark.udf().register("getWeightListWEP", new GetWeightListWEP(), DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)));

//		Dataset<Row> df = spark.read().json("data/blocks.json");
		Dataset<Row> df1 = spark.read().format("csv").option("header", "true").load("data/AmazonGoogle/Amazon.csv");
		
		
		df1 = blocking(df1);
		System.out.println("Before transformation:");
		df1.show(false);

		/*
		 * Calculates the cardinality of each block and creates a new column to save the
		 * data
		 */
		Dataset<Row> dfmapped = df1.withColumn("ofEntities", size(df1.col("entities")))
				.withColumn("cardinality", expr("int(((ofEntities-1)*ofEntities)/2)")).drop("ofEntities");

		/*
		 * Sorting the columns according to cardinality and breaks the arrays of
		 * entities
		 */
		dfmapped = dfmapped.sort(dfmapped.col("cardinality").asc())
				.withColumn("entityId", explode(dfmapped.col("entities"))).drop("entities");
		/*
		 * Groups data by entityID to get rid of duplicates and collect in a list the
		 * blocks that are associated with
		 */
		dfmapped = dfmapped.groupBy("entityId").agg(collect_list(dfmapped.col("block")).alias("AssociatedBlocks"))
				.sort(dfmapped.col("entityId").asc()); // Maybe it will need a monotonically_increasing_id() to get the
														// top N BlocksId

		System.out.println("After transformation:");
		dfmapped.show(false);
		dfmapped.printSchema();
		Dataset<Row> dfTest = dfmapped;
		System.out.println(
				"==============================================================================================");

		// Stage 2 Preprocessing
		System.out.println("Start of Preprocessing ");
		Dataset<Row> dfPreprocessing = dfmapped.withColumn("BlockId", explode(dfmapped.col("AssociatedBlocks")))
				.drop("AssociatedBlocks");
		dfPreprocessing = dfPreprocessing.groupBy("BlockId").agg(collect_list("entityId").as("EntityList"));
		dfPreprocessing = dfPreprocessing.withColumn("listSize", size(dfPreprocessing.col("EntityList")));
		dfPreprocessing = dfPreprocessing.filter(col("listSize").geq(2)).drop("listSize");
		dfPreprocessing.show(false);
		System.out.println("End of Preprocessing");

		System.out
				.println("==========================================================================================");
		// Stage 3 Pruning
		// WEP

		Dataset<Row> dfWEP = dfPreprocessing.withColumn("EntityId", explode(dfPreprocessing.col("EntityList")))
				.drop("BlockId");
		dfWEP = dfWEP.select("EntityId", "EntityList");
		dfWEP = dfWEP.withColumnRenamed("EntityList", "CoOccurrenceBag");
		dfWEP.show(false);

		dfWEP = dfWEP.groupBy("EntityId").agg(flatten(collect_list("CoOccurrenceBag")).as("CoOccurrenceBag"));
		dfWEP.show(false);

		dfWEP = dfWEP.withColumn("numberOfEntities", lit((int) dfWEP.count()));
		// The array frequencies contains for eve
		dfWEP = dfWEP
				.withColumn("Frequencies",
						callUDF("getFrequencies", dfWEP.col("CoOccurrenceBag"), dfWEP.col("numberOfEntities"))
								.cast(DataTypes.createArrayType(DataTypes.LongType)))
				.withColumn("SetOfNeighbors", array_distinct(dfWEP.col("CoOccurrenceBag"))).drop("numberOfEntities");
		dfWEP = dfWEP.withColumn("SetOfNeighborsWithoutID",
				array_remove(dfWEP.col("SetOfNeighbors"), dfWEP.col("EntityId")));
		dfWEP = dfWEP.sort("EntityId");
		dfWEP.show(false);

		dfTest = dfTest.withColumn("arraysize", size(dfTest.col("AssociatedBlocks"))).withColumnRenamed("entityId",
				"ent2");
		
		// Prepei na dior8wsw auto to kommati =============================================================
		List<Row> test = dfTest.select("arraysize").collectAsList();

		ArrayList<Integer> testlist = new ArrayList<Integer>();
		test.forEach(m -> testlist.add((Integer) m.get(0)));
		dfTest.show(false);

		String str = testlist.toString();
		str = str.substring(1, str.length() - 1);
		System.out.println(str);

		dfWEP = dfWEP.withColumn("NumberOfBlocks",
				split(lit(str), ", ").cast(DataTypes.createArrayType(DataTypes.LongType)));
		//================================================================================================
		
		
		// dfWEP = dfWEP.withColumn("Weight", callUDF("getWeight",
		// dfWEP.col("Frequencies"), dfWEP.col("SetOfNeighborsWithoutID"),
		// dfWEP.col("EntityId")));
		dfWEP = dfWEP.withColumn("Weight", callUDF("GetTotalWeightWEP", dfWEP.col("Frequencies"),
				dfWEP.col("SetOfNeighborsWithoutID"), dfWEP.col("NumberOfBlocks"), dfWEP.col("EntityId")))
				.withColumn("NumberOfEdges", callUDF("GetNumberOfEdgesWEP", dfWEP.col("Frequencies"),
						dfWEP.col("SetOfNeighborsWithoutID"), dfWEP.col("NumberOfBlocks"), dfWEP.col("EntityId")));
		dfWEP.show(false);
		dfWEP.printSchema();
		dfWEP = dfWEP.withColumn("WeightList", callUDF("GetWeightListWEP", dfWEP.col("Frequencies"),
				dfWEP.col("SetOfNeighborsWithoutID"), dfWEP.col("NumberOfBlocks"), dfWEP.col("EntityId"), dfWEP.col("Weight"), dfWEP.col("NumberOfEdges")))
				.drop("CoOccurrenceBag")
				.drop("Frequencies")
				.drop("SetOfNeighbors")
				.drop("SetOfNeighborsWithoutID")
				.drop("NumberOfBlocks")
				.drop("Weight")
				.drop("NumberOfEdges");
		dfWEP.show(false);
		dfWEP = dfWEP.filter(size(col("WeightList")).gt(0)).select("EntityId", "WeightList");
		dfWEP = dfWEP.withColumn("jEntity_Weight", explode(dfWEP.col("WeightList"))).drop("WeightList");
		dfWEP.show(false);
		dfWEP = dfWEP.withColumn("Edge", array(dfWEP.col("EntityId"), dfWEP.col("jEntity_Weight").getItem(0).cast(DataTypes.LongType)))
				.withColumn("Weight", dfWEP.col("jEntity_Weight").getItem(1))
				.drop("jEntity_Weight").drop("EntityId");
		dfWEP.select("Edge", "Weight").show(false);

	}
	private static Dataset<Row> blocking(Dataset<Row> df){
		df = df.withColumn("entityid", row_number().over(Window.orderBy(lit(1)))).drop("description").drop("manufacturer").drop("price").drop("id");
		df = df.withColumn("entityid", df.col("entityid").cast(DataTypes.LongType));
		df = df.withColumn("title", split(df.col("title"), " "));
		df = df.withColumn("titleTokens", explode(df.col("title"))).drop("title");
		df = df.groupBy("titletokens").agg(collect_list("entityid").as("entities"));
		df = df.withColumn("block", row_number().over(Window.orderBy(lit(1)))).drop("titletokens");
		df = df.withColumn("block", df.col("block").cast(DataTypes.LongType));
		df = df.select("block", "entities");
		df.show(false);
		df.printSchema();
		df.cache();
		return df;
	}

}