package com.turn.splicer.tsdbutils.expression;

import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.TsQuery;

import java.util.List;

public interface Expression {

	TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> queryParams);

	String writeStringField(List<String> queryParams, String innerExpression);

}
