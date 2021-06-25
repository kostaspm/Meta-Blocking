package comparisonBasedStrategy;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF3;

import scala.collection.mutable.WrappedArray;

public class GetWeightUDFECBS implements UDF3<WrappedArray<Long>, WrappedArray<Long>, Integer, ArrayList<Double>> {

	private static final long serialVersionUID = -2162175912L;

	@Override
	public ArrayList<Double> call(WrappedArray<Long> blockSize, WrappedArray<Long> commonBlocks, Integer numberOfBlocks)
			throws Exception {
		// TODO Auto-generated method stub
		ArrayList<Double> weights = new ArrayList<Double>();
		int commonBlocksCounter = 0;
		for (int i = 0; i < blockSize.length(); i++) {
			Double iEntityBlockSize = (double) blockSize.apply(i);
			for (int j = i + 1; j < blockSize.length(); j++) {
				Double commonBlocksSize = (double) commonBlocks.apply(commonBlocksCounter);
				Double jEntityBlockSize = (double) blockSize.apply(j);
				Double weight = commonBlocksSize * Math.log10(numberOfBlocks / iEntityBlockSize)
						* Math.log10(numberOfBlocks / jEntityBlockSize);
				weights.add(weight);
				commonBlocksCounter++;
			}
		}

		return weights;
	}

}
