package com.turn.splicer.tsdbutils.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.Functions;
import com.turn.splicer.tsdbutils.TsQuery;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

public class ExpressionFactory {

	private static final Logger logger = Logger.getLogger(ExpressionFactory.class);

	private static Map<String, Expression> availableFunctions =
			Maps.newHashMap();

	static {
		availableFunctions.put("id", new IdentityExpression());
		availableFunctions.put("alias", new AliasFunction());
		availableFunctions.put("scale", new Functions.ScaleFunction());
		availableFunctions.put("sumSeries", new Functions.SumSeriesFunction());
		availableFunctions.put("sum", new Functions.SumSeriesFunction());
		availableFunctions.put("difference", new Functions.DifferenceSeriesFunction());
		availableFunctions.put("multiply", new Functions.MultiplySeriesFunction());
		availableFunctions.put("divide", new Functions.DivideSeriesFunction());
		availableFunctions.put("movingAverage", new Functions.MovingAverageFunction());
		availableFunctions.put("highestCurrent", new Functions.HighestCurrent());
		availableFunctions.put("highestMax", new Functions.HighestMax());
		availableFunctions.put("abs", new Functions.AbsoluteValueFunction());
	}

	@VisibleForTesting
	static void addFunction(String name, Expression expr) {
		availableFunctions.put(name, expr);
	}

	public static Expression getByName(String funcName) {
		return availableFunctions.get(funcName);
	}

	static class IdentityExpression implements Expression {
		@Override
		public TsdbResult[] evaluate(TsQuery data_query,
		                             List<TsdbResult[]> queryResults, List<String> queryParams) {
			return queryResults.get(0);
		}

		@Override
		public String toString() {
			return "id";
		}

		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "id(" + innerExpression + ")";
		}
	}

	static class AliasFunction implements Expression {

		static Joiner COMMA_JOINER = Joiner.on(',').skipNulls();

		@Override
		public TsdbResult[] evaluate(TsQuery data_query, List<TsdbResult[]> queryResults,
		                             List<String> queryParams) {
			if (queryResults == null || queryResults.size() == 0) {
				throw new NullPointerException("No query results");
			}

			String aliasTemplate = "__default";

			if (queryParams != null && queryParams.size() >= 0) {
				aliasTemplate = COMMA_JOINER.join(queryParams);
			}

			TsdbResult[] resultToAlias = queryResults.get(0);

			for(TsdbResult tsResult: resultToAlias) {
				String alias = aliasTemplate;
				for(Map.Entry<String, String> tagKVPair: tsResult.getTags().getTags().entrySet()) {
					alias = alias.replace("@" + tagKVPair.getKey(), tagKVPair.getValue());
				}
				tsResult.setAlias(alias);
			}

			return resultToAlias;
		}

		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			if (queryParams == null || queryParams.size() == 0) {
				return "NULL";
			}

			return queryParams.get(0);
		}
	}
}
