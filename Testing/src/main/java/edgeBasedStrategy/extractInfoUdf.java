package edgeBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class extractInfoUdf implements UDF1<WrappedArray<Integer>, ArrayList<ArrayList<Integer>>> {
	private static Logger log = LoggerFactory.getLogger(createPairsUdf.class);
	private static final long serialVersionUID = -21621751L;

	public ArrayList<ArrayList<Integer>> call(WrappedArray<Integer> entities) throws Exception {
		log.debug("-> call({}, {})", entities);
		ArrayList<ArrayList<Integer>> pairs = new ArrayList<ArrayList<Integer>>();

		for (int i = 0; i < entities.length(); i++) {
			int num1 = entities.apply(i);
			for (int j = i + 1; j < entities.length(); j++) {
				ArrayList<Integer> pair = new ArrayList<Integer>();
				int num2 = entities.apply(j);
				pair.add(num1);
				pair.add(num2);
				pairs.add(pair);
				
			}
		}
		return pairs;
	}
}