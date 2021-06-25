package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF3;

import scala.collection.mutable.WrappedArray;

public class GetTotalWeightWEPCBS
		implements UDF3<WrappedArray<Long>, WrappedArray<Long>, Long, Double> {

	private static final long serialVersionUID = -7469620826868963887L;

	public Double call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities,
			Long iEntity) throws Exception {

		double totalWeight = 0.0;

		for (int i = 0; i < jEntities.length(); i++) {

			if (jEntities.apply(i) < iEntity) {
				Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1);

				double currentWeight = commonBlocks;
				totalWeight += currentWeight;
			}
		}

		return totalWeight;
	}

}
