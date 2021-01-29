package edgeBasedStrategy;

import org.apache.spark.sql.api.java.UDF2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class averageWeightUdf implements UDF2 <WrappedArray<Double>, Integer, Double> {
	
	private static Logger log = LoggerFactory.getLogger(averageWeightUdf.class);
	private static final long serialVersionUID = -21621754L;
	
	public Double call(WrappedArray<Double> weights, Integer size) throws Exception {
		log.debug("-> call({}, {})", weights);
		double sum = 0;

		for (int i = 0; i < weights.length(); i++) {
			sum += weights.apply(i);
		}
		return sum / size;
	}

}
