package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF3;

import scala.collection.mutable.WrappedArray;

public class GetNumberOfEdgesWEP implements UDF3<WrappedArray<Long>, WrappedArray<Long>, Long, Long> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7469620826868963887L;

	public Long call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, Long iEntity) throws Exception {

		Long numberOfEdges = 0L;

		for (int i = 0; i < jEntities.length(); i++) {

			if (jEntities.apply(i) < iEntity) {
				numberOfEdges++;
			}
		}

		return numberOfEdges;
	}
}
