package entityBasedStrategy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public 	class GetFrequenciesUDF implements UDF2 <WrappedArray<Long>, Integer, List<Integer>> {
	
	private static Logger log = LoggerFactory.getLogger(GetFrequenciesUDF.class);
	private static final long serialVersionUID = -21621754L;
	

	public List<Integer> call(WrappedArray<Long> bag, Integer maxElement) throws Exception {
		log.debug("-> call({}, {})", bag);
		List<Integer> frequencies = new ArrayList<Integer>(Collections.nCopies(maxElement, 0));
		for(int i = 0; i < bag.length(); i++){
			frequencies.set((int) (bag.apply(i)-1), frequencies.get((int) (bag.apply(i)-1) ) + 1);
		}
		return frequencies;
	}
}