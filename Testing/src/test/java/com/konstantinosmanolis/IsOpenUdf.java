package com.konstantinosmanolis;

import java.sql.Timestamp;

import org.apache.spark.sql.api.java.UDF8;

public class IsOpenUdf implements UDF8<String, String, String, String, String, String, String, Timestamp, Boolean> {
	private static final long serialVersionUID = -216751L;

	@Override
	public Boolean call(String hoursMon, String hoursTue, String hoursWed, String hoursThu, String hoursFri,
			String hoursSat, String hoursSun, Timestamp dateTime) throws Exception {

		return IsOpenService.isOpen(hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun, dateTime);
	}

}
