package entityBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.array_remove;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.slice;
import static org.apache.spark.sql.functions.desc;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EntityBasedCNP {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Comparison Based Strategy").config("spark.master", "local")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		spark.udf().register("getFrequencies", new getFrequencies(), DataTypes.createArrayType(DataTypes.IntegerType));

		spark.udf().register("getWeightCNP", new GetWeightCNP(), DataTypes.createArrayType(DataTypes.DoubleType));
		spark.udf().register("filterNodes", new FilterNodesUDF(), DataTypes.createArrayType(DataTypes.LongType));

		Dataset<Row> df = spark.read().json("data/blocks.json");
		System.out.println("Before transformation:");
		df.show(false);

		/*
		 * Calculates the cardinality of each block and creates a new column to save the
		 * data
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
		// CNP

		Dataset<Row> dfCNP = dfPreprocessing.withColumn("EntityId", explode(dfPreprocessing.col("EntityList")))
				.drop("BlockId");
		dfCNP = dfCNP.select("EntityId", "EntityList");
		dfCNP = dfCNP.withColumnRenamed("EntityList", "CoOccurrenceBag");
		dfCNP.show(false);

		dfCNP = dfCNP.groupBy("EntityId").agg(flatten(collect_list("CoOccurrenceBag")).as("CoOccurrenceBag"));
		dfCNP.show(false);

		dfCNP = dfCNP.withColumn("numberOfEntities", lit((int) dfCNP.count()));
		// The array frequencies contains for eve
		dfCNP = dfCNP
				.withColumn("Frequencies",
						callUDF("getFrequencies", dfCNP.col("CoOccurrenceBag"), dfCNP.col("numberOfEntities"))
								.cast(DataTypes.createArrayType(DataTypes.LongType)))
				.withColumn("SetOfNeighbors", array_distinct(dfCNP.col("CoOccurrenceBag"))).drop("numberOfEntities");
		dfCNP = dfCNP.withColumn("SetOfNeighborsWithoutID",
				array_remove(dfCNP.col("SetOfNeighbors"), dfCNP.col("EntityId")));
		dfCNP = dfCNP.sort("EntityId");
		dfCNP.show(false);

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

		dfCNP = dfCNP.withColumn("NumberOfBlocks",
				split(lit(str), ", ").cast(DataTypes.createArrayType(DataTypes.LongType)));
		// ================================================================================================
		
		
		dfCNP = dfCNP.withColumn("NumberOfEdges", size(col("SetOfNeighborsWithoutID")));

		dfCNP = dfCNP
				.withColumn("WeightList",
						callUDF("getWeightCNP", dfCNP.col("Frequencies"), dfCNP.col("SetOfNeighborsWithoutID"),
								dfCNP.col("NumberOfBlocks"), dfCNP.col("EntityId")))
				.drop("CoOccurrenceBag").drop("Frequencies").drop("SetOfNeighbors").drop("NumberOfBlocks")
				.drop("NumberOfEdges");
		dfCNP.show(false);
		
		String transform_expr = "transform(SetOfNeighborsWithoutID, (x, i) -> struct(x as element1, WeightList[i] as element2))";
		dfCNP = dfCNP.withColumn("merged_arrays", explode(expr(transform_expr)))
				.withColumn("jEntity", col("merged_arrays.element1"))
				.withColumn("Weight", col("merged_arrays.element2")).drop("merged_arrays")
				.drop("SetOfNeighborsWithoutID").drop("WeightList");

		dfCNP = dfCNP.orderBy(desc("Weight")).groupBy("EntityId").agg(
				collect_list(dfCNP.col("jEntity")).as("jEntityList"),
				collect_list(dfCNP.col("Weight")).as("weightsList"));
		dfCNP = dfCNP.withColumn("jEntityList", slice(dfCNP.col("jEntityList"), 1, 3)).withColumn("weightsList",
				slice(dfCNP.col("weightsList"), 1, 3)); // change 3rd argument to match the Top k weights
		dfCNP = dfCNP
				.withColumn("merged_arrays",
						explode(expr(
								"transform(jEntityList, (x, i) -> struct(x as element1, weightsList[i] as element2))")))
				.withColumn("jEntity", col("merged_arrays.element1"))
				.withColumn("Weight", col("merged_arrays.element2")).drop("merged_arrays").drop("weightsList")
				.drop("jEntityList");
		dfCNP = dfCNP.withColumn("Edge", array(dfCNP.col("EntityId"), dfCNP.col("jEntity"))).drop("EntityId")
				.drop("jEntity");
		// Μένει να βγάλω τα διπλότυπα
		dfCNP.show(false);
	}

}
