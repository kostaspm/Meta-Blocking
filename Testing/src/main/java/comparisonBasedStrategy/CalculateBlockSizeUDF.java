package comparisonBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF1;

import scala.collection.mutable.WrappedArray;

public class CalculateBlockSizeUDF implements UDF1 <WrappedArray<WrappedArray<Long>>, ArrayList<Long>> {

	private static final long serialVersionUID = -216217592L;
	
	public ArrayList<Long> call(WrappedArray<WrappedArray<Long>> associatedBlocks) throws Exception {
		ArrayList<Long> size = new ArrayList<Long>();
		for(int i = 0; i < associatedBlocks.length(); i++){
			size.add((long) associatedBlocks.apply(i).length());
			}
		return size;
	}
}