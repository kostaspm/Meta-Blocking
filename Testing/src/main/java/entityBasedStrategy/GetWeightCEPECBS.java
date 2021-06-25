package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF5;

import scala.collection.mutable.WrappedArray;

public class GetWeightCEPECBS implements
		UDF5<WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, Integer, ArrayList<ArrayList<Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8618285765926468296L;

	public ArrayList<ArrayList<Double>> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities,
			WrappedArray<Long> NumberOfBlocks, Long iEntity, Integer BlockSize) throws Exception {

		ArrayList<ArrayList<Double>> list = new ArrayList<ArrayList<Double>>();

		for (int i = 0; i < jEntities.length(); i++) {

			if (jEntities.apply(i) < iEntity) {
				Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1);
				Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
				Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);

				Double currentWeight = commonBlocks * Math.log10(BlockSize / iCardinality)
						* Math.log10(BlockSize / jCardinality);
				ArrayList<Double> entityWeight = new ArrayList<Double>();
				entityWeight.add((double) jEntities.apply(i));
				entityWeight.add(currentWeight);
				list.add(entityWeight);
			}
		}

		return list;
	}
}
