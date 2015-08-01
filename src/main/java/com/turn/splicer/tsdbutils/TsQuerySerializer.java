package com.turn.splicer.tsdbutils;

import com.google.common.base.Preconditions;

public class TsQuerySerializer {

	public static TsQuery deserializeFromJson(String jsonContent)
	{
		Preconditions.checkNotNull(jsonContent);
		Preconditions.checkArgument(!jsonContent.isEmpty(), "Incoming data was null or empty");

		return JSON.parseToObject(jsonContent, TsQuery.class);
	}

}
