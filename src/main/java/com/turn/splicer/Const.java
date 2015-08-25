package com.turn.splicer;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Const {

	public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");

	public static String tsFormat(long val) {
		return SDF.format(new Date(val));
	}

	/**
	 * Mask to verify a timestamp on 6 bytes in milliseconds
	 */
	public static final long MILLISECOND_MASK = 0xFFFFF00000000000L;

}
