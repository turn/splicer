package com.turn.splicer.hbase;

public class MetricLookupException extends RuntimeException {

	public MetricLookupException(String msg, Exception e) {
		super(msg, e);
	}

}
