package com.turn.splicer.merge;

public class MergeException extends RuntimeException {

	public MergeException(String msg) {
		super(msg);
	}

	public MergeException(String msg, Exception e) {
		super(msg, e);
	}
}
