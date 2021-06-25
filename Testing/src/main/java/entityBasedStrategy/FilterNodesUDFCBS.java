package entityBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class FilterNodesUDFCBS
		implements UDF3<WrappedArray<Long>, WrappedArray<Long>, Double, ArrayList<ArrayList<Double>>> {

	private static Logger log = LoggerFactory.getLogger(getFrequencies.class);
	private static final long serialVersionUID = -2162175894L;

	public ArrayList<ArrayList<Double>> call(WrappedArray<Long> frequencies, WrappedArray<Long> jEntities,
			Double NeighborhoodWeight) throws Exception {
		log.debug("-> call({}, {})", frequencies);

		ArrayList<ArrayList<Double>> filteredEntities = new ArrayList<ArrayList<Double>>();

		for (int i = 0; i < jEntities.length(); i++) {
			ArrayList<Double> entityWeightList = new ArrayList<Double>();
			Double commonBlocks = (double) frequencies.apply(jEntities.apply(i).intValue() - 1);

			Double currentWeight = commonBlocks;
			if (NeighborhoodWeight < currentWeight) {
				double entity = jEntities.apply(i).doubleValue();
				entityWeightList.add(entity);
				entityWeightList.add(currentWeight);
				filteredEntities.add(entityWeightList);

			}
		}

		return filteredEntities;
	}

}
