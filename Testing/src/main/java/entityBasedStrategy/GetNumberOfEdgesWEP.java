package entityBasedStrategy;

import org.apache.spark.sql.api.java.UDF4;

import scala.collection.mutable.WrappedArray;

public class GetNumberOfEdgesWEP implements UDF4 <WrappedArray<Long>, WrappedArray<Long>, WrappedArray<Long>, Long, Long> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7469620826868963887L;

	public Long call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities, WrappedArray<Long> NumberOfBlocks, Long iEntity) throws Exception {
		
		Long numberOfEdges = 0L;
		
		for(int i = 0; i < jEntities.length(); i++){
			
			if(jEntities.apply(i) < iEntity) {
				numberOfEdges++;
			}
		}
		
		return numberOfEdges;
	}
}

