package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF4;

import scala.collection.mutable.WrappedArray;

public class GetWeightCEP implements
		UDF4<WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, ArrayList<ArrayList<Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8618285765926468296L;

	public ArrayList<ArrayList<Double>> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities,
			WrappedArray<Long> NumberOfBlocks, Long iEntity) throws Exception {

		ArrayList<ArrayList<Double>> list = new ArrayList<ArrayList<Double>>();

		for (int i = 0; i < jEntities.length(); i++) {

			if (jEntities.apply(i) < iEntity) {
				Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1);
				Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
				Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);

				double currentWeight = jaccardScheme(iCardinality, jCardinality, commonBlocks);
				ArrayList<Double> entityWeight = new ArrayList<Double>();
				entityWeight.add((double) jEntities.apply(i));
				entityWeight.add(currentWeight);
				list.add(entityWeight);
			}
		}

		return list;
	}

	private double jaccardScheme(Double iCardinality, Double jCardinality, Double commonBlocks) {
		double weight = commonBlocks / (iCardinality + jCardinality - commonBlocks);
		return weight;
	}

}
