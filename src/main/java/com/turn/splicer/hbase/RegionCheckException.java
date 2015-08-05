package com.turn.splicer.hbase;

public class RegionCheckException extends RuntimeException {

	public RegionCheckException(String msg) {
		super(msg);
	}

	public RegionCheckException(String msg, Exception e) {
		super(msg, e);
	}
}
