package entityBasedStrategy;

import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.array_remove;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_distinct;
import static org.apache.spark.sql.functions.flatten;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EntityBasedWNP {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Comparison Based Strategy").config("spark.master", "local")
				.getOrCreate();
		
		spark.sparkContext().setLogLevel("ERROR");
		//spark.udf().register("extractInfo", new extractInfoUdf(),
		//		DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.LongType)));
		spark.udf().register("getFrequencies", new getFrequenciesUDF(),
				DataTypes.createArrayType(DataTypes.IntegerType));
		spark.udf().register("getWeight", new GetWeight(), DataTypes.DoubleType);
		spark.udf().register("filterNodes", new FilterNodesUDF(), DataTypes.createArrayType(DataTypes.LongType));
		
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
		Dataset<Row> dfTest = dfmapped;
		System.out.println(
				"==============================================================================================");
		
		// Stage 2 Preprocessing
		System.out.println("Start of Preprocessing ");
		Dataset<Row> dfPreprocessing = dfmapped.withColumn("BlockId", explode(dfmapped.col("AssociatedBlocks"))).drop("AssociatedBlocks");
		dfPreprocessing = dfPreprocessing.groupBy("BlockId").agg(collect_list("entityId").as("EntityList"));
		dfPreprocessing = dfPreprocessing.withColumn("listSize", size(dfPreprocessing.col("EntityList")));
		dfPreprocessing = dfPreprocessing.filter(col("listSize").geq(2)).drop("listSize");
		dfPreprocessing.show(false);
		System.out.println("End of Preprocessing");
		
		System.out.println("==========================================================================================");
		// Stage 3 Pruning
		// WNP
		
		Dataset<Row> dfWNP = dfPreprocessing.withColumn("EntityId", explode(dfPreprocessing.col("EntityList"))).drop("BlockId");
		dfWNP = dfWNP.select("EntityId", "EntityList");
		dfWNP = dfWNP.withColumnRenamed("EntityList", "CoOccurrenceBag");
		dfWNP.show(false);
		
		dfWNP = dfWNP.groupBy("EntityId").agg(flatten(collect_list("CoOccurrenceBag")).as("CoOccurrenceBag"));
		dfWNP.show(false);
		
		dfWNP = dfWNP.withColumn("numberOfEntities", lit((int)dfWNP.count()));
		// The array frequencies contains for eve
		dfWNP = dfWNP.withColumn("Frequencies", callUDF("getFrequencies", dfWNP.col("CoOccurrenceBag"), dfWNP.col("numberOfEntities")).cast(DataTypes.createArrayType(DataTypes.LongType))).withColumn("SetOfNeighbors", array_distinct(dfWNP.col("CoOccurrenceBag"))).drop("numberOfEntities");
		dfWNP = dfWNP.withColumn("SetOfNeighborsWithoutID", array_remove(dfWNP.col("SetOfNeighbors"), dfWNP.col("EntityId")));
		dfWNP = dfWNP.sort("EntityId");
		dfWNP.show(false);
		
		dfTest = dfTest.withColumn("arraysize", size(dfTest.col("AssociatedBlocks"))).withColumnRenamed("entityId", "ent2");
		
		// Better Join
		List<Row> test = dfTest.select("arraysize").collectAsList();
		

		ArrayList<Integer> testlist = new ArrayList<Integer>();
		test.forEach(m -> testlist.add((Integer) m.get(0)));
		dfTest.show(false);
		
		String str = testlist.toString();
		str = str.substring(1, str.length() - 1);
		System.out.println(str);
		
		
		
		dfWNP = dfWNP.withColumn("NumberOfBlocks", split(lit(str), ", ").cast(DataTypes.createArrayType(DataTypes.LongType)) );
		dfWNP = dfWNP.withColumn("NumberOfEdges", size(col("SetOfNeighborsWithoutID")));

		dfWNP = dfWNP.withColumn("Weight", callUDF("getWeight", dfWNP.col("Frequencies"), dfWNP.col("SetOfNeighborsWithoutID"),dfWNP.col("NumberOfBlocks"), dfWNP.col("EntityId")));

		dfWNP = dfWNP.withColumn("AverageWeight", expr("Weight / NumberOfEdges")).drop("NumberOfEdges").drop("CoOccurrenceBag").drop("Weight");
		dfWNP.cache();
		dfWNP.show(false);
		dfWNP.printSchema();
		dfWNP = dfWNP.withColumn("FilteredNeighborhood", callUDF("filterNodes", dfWNP.col("Frequencies"), dfWNP.col("SetOfNeighborsWithoutID"),dfWNP.col("NumberOfBlocks"), dfWNP.col("EntityId"), dfWNP.col("AverageWeight")))
				.drop("Frequencies").drop("SetOfNeighbors").drop("SetOfNeighborsWithoutID").drop("NumberOfBlocks").drop("AverageWeight");
		dfWNP.show(false);
		dfWNP = dfWNP.withColumn("FilteredNeighborhood", explode(dfWNP.col("FilteredNeighborhood")));
		dfWNP.show(false);
		dfWNP = dfWNP.withColumn("Edge", array(dfWNP.col("EntityId"), dfWNP.col("FilteredNeighborhood")));
		dfWNP.show(false);
		
	}

}



class getFrequenciesUDF implements UDF2 <WrappedArray<Long>, Integer, List<Integer>> {
	
	private static Logger log = LoggerFactory.getLogger(getFrequenciesUDF.class);
	private static final long serialVersionUID = -21621754L;
	
	public List<Integer> call(WrappedArray<Long> bag, Integer NumberOfEntities) throws Exception {
		log.debug("-> call({}, {})", bag);
		List<Integer> frequencies = new ArrayList<Integer>(Collections.nCopies(NumberOfEntities, 0));
		for(int i = 0; i < bag.length(); i++){
			frequencies.set((int) (bag.apply(i)-1), frequencies.get((int) (bag.apply(i)-1) ) + 1);
		}
		return frequencies;
	}

}