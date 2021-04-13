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
import static org.apache.spark.sql.functions.monotonically_increasing_id;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.desc;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

public class EntityBasedCEP {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Comparison Based Strategy").config("spark.master", "local")
				.getOrCreate();

		spark.sparkContext().setLogLevel("ERROR");
		spark.udf().register("getFrequencies", new getFrequencies(), DataTypes.createArrayType(DataTypes.IntegerType));
		spark.udf().register("getWeightCEP", new GetWeightCEP(), DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.DoubleType)));

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
		// WEP

		Dataset<Row> dfCEP = dfPreprocessing.withColumn("EntityId", explode(dfPreprocessing.col("EntityList")))
				.drop("BlockId");
		dfCEP = dfCEP.select("EntityId", "EntityList");
		dfCEP = dfCEP.withColumnRenamed("EntityList", "CoOccurrenceBag");
		dfCEP.show(false);

		dfCEP = dfCEP.groupBy("EntityId").agg(flatten(collect_list("CoOccurrenceBag")).as("CoOccurrenceBag"));
		dfCEP.show(false);

		dfCEP = dfCEP.withColumn("numberOfEntities", lit((int) dfCEP.count()));
		// The array frequencies contains for eve
		dfCEP = dfCEP
				.withColumn("Frequencies",
						callUDF("getFrequencies", dfCEP.col("CoOccurrenceBag"), dfCEP.col("numberOfEntities"))
								.cast(DataTypes.createArrayType(DataTypes.LongType)))
				.withColumn("SetOfNeighbors", array_distinct(dfCEP.col("CoOccurrenceBag"))).drop("numberOfEntities");
		dfCEP = dfCEP.withColumn("SetOfNeighborsWithoutID",
				array_remove(dfCEP.col("SetOfNeighbors"), dfCEP.col("EntityId")));
		dfCEP = dfCEP.sort("EntityId");
		dfCEP.show(false);

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

		dfCEP = dfCEP.withColumn("NumberOfBlocks",
				split(lit(str), ", ").cast(DataTypes.createArrayType(DataTypes.LongType)));
		//================================================================================================
		
		dfCEP.show(false);
		dfCEP = dfCEP.withColumn("Weight", callUDF("GetWeightCEP", dfCEP.col("Frequencies"),
				dfCEP.col("SetOfNeighborsWithoutID"), dfCEP.col("NumberOfBlocks"), dfCEP.col("EntityId")));
		dfCEP.show(false);
		
		Dataset<Row> dfCEPtest = dfCEP.select("Weight");
		dfCEPtest = dfCEPtest.withColumn("Weight", explode(dfCEPtest.col("Weight")));
		dfCEPtest = dfCEPtest.withColumn("jEntity", dfCEPtest.col("Weight").getItem(0)).withColumn("Weight", dfCEPtest.col("Weight").getItem(1))
				.sort(desc("Weight"));
		dfCEPtest = dfCEPtest.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id())));
		dfCEPtest = dfCEPtest.filter(col("id").equalTo(7)).select("Weight"); //equal to k-position
		
		double valueOfMinWeight = dfCEPtest.head().getDouble(0);
		
		
		dfCEP = dfCEP.select("EntityId", "Weight").withColumn("Weight", explode(col("Weight")));
		
		dfCEP = dfCEP.withColumn("Edge", array(col("EntityId"), dfCEP.col("Weight").getItem(0).cast(DataTypes.LongType))).drop("EntityId").withColumn("Weight", col("Weight").getItem(1));
		dfCEP = dfCEP.filter(col("Weight").geq(valueOfMinWeight));
		
		dfCEP.show(false);
	}

}