package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class GetWeightCNPECBS
		implements UDF5<WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, Integer, ArrayList<Double>> {

	private static Logger log = LoggerFactory.getLogger(getFrequencies.class);
	private static final long serialVersionUID = -21621754L;

	public ArrayList<Double> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities,
			WrappedArray<Long> NumberOfBlocks, Long iEntity, Integer BlockSize) throws Exception {
		log.debug("-> call({}, {})", frequencies);

		ArrayList<Double> weights = new ArrayList<Double>();

		for (int i = 0; i < jEntities.length(); i++) {

			Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1);
			Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
			Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);

			Double currentWeight = commonBlocks * Math.log10(BlockSize / iCardinality)
					* Math.log10(BlockSize / jCardinality);
			weights.add(currentWeight);

		}

		return weights;
	}
}
