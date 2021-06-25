package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF5;

import scala.collection.mutable.WrappedArray;

public class GetTotalWeightWEPECBS
		implements UDF5<WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, Integer, Double> {

	private static final long serialVersionUID = -7469620826868963887L;

	public Double call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, WrappedArray<Long> NumberOfBlocks,
			Long iEntity, Integer BlockSize) throws Exception {

		double totalWeight = 0.0;

		for (int i = 0; i < jEntities.length(); i++) {

			if (jEntities.apply(i) < iEntity) {
				Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1);
				Double iCardinality = (double) NumberOfBlocks.apply(iEntity.intValue() - 1);
				Double jCardinality = (double) NumberOfBlocks.apply(jEntities.apply(i).intValue() - 1);

				Double currentWeight = commonBlocks * Math.log10(BlockSize / iCardinality)
						* Math.log10(BlockSize / jCardinality);
				totalWeight += currentWeight;
			}
		}

		return totalWeight;
	}

}
