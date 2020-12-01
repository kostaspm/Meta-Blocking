package com.konstantinosmanolis;

import java.util.ArrayList;

import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.mutable.WrappedArray;

public class extractInfoUdf implements UDF1<WrappedArray<WrappedArray<Long>>, ArrayList<ArrayList<Long>>>{
	private static Logger log = LoggerFactory.getLogger(extractInfoUdf.class);
	private static final long serialVersionUID = -21621752L;
	
	@Override
	public ArrayList<ArrayList<Long>> call(WrappedArray<WrappedArray<Long>> entities) throws Exception {
		log.debug("-> call({}, {})", entities);
		ArrayList<ArrayList<Long>> pairs = new ArrayList<>();

		for (int i = 0; i < entities.length(); i++) {
			Long num1 = entities.apply(i).apply(1);
			for (int j = i + 1; j < entities.length(); j++) {
				ArrayList<Long> pair = new ArrayList<>();
				Long num2 = entities.apply(j).apply(1);
				pair.add(num1);
				pair.add(num2);
				pairs.add(pair);
			}
		}
		return pairs;
	}
}
