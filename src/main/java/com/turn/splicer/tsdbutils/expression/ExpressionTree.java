package com.turn.splicer.tsdbutils.expression;

import com.turn.splicer.tsdbutils.TsQuery;
import com.turn.splicer.merge.TsdbResult;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ExpressionTree {

	private final Expression expr;
	private final TsQuery dataQuery;

	private List<ExpressionTree> subExpressions;
	private List<String> funcParams;
	private Map<Integer, String> subMetricQueries;
	private Map<Integer, Parameter> parameterSourceIndex = Maps.newHashMap();

	private static final Joiner DOUBLE_COMMA_JOINER = Joiner.on(",").skipNulls();

	enum Parameter {
		SUB_EXPRESSION,
		METRIC_QUERY
	}

	public ExpressionTree(String exprName, TsQuery dataQuery) {
		this(ExpressionFactory.getByName(exprName), dataQuery);
	}

	public ExpressionTree(Expression expr, TsQuery dataQuery) {
		this.expr = expr;
		this.dataQuery = dataQuery;
	}

	public void addSubExpression(ExpressionTree child, int paramIndex) {
		if (subExpressions == null) {
			subExpressions = Lists.newArrayList();
		}
		subExpressions.add(child);
		parameterSourceIndex.put(paramIndex, Parameter.SUB_EXPRESSION);
	}

	public void addSubMetricQuery(String metricQuery, int magic,
	                              int paramIndex) {
		if (subMetricQueries == null) {
			subMetricQueries = Maps.newHashMap();
		}
		subMetricQueries.put(magic, metricQuery);
		parameterSourceIndex.put(paramIndex, Parameter.METRIC_QUERY);
	}

	public void addFunctionParameter(String param) {
		if (funcParams == null) {
			funcParams = Lists.newArrayList();
		}
		funcParams.add(param);
	}

	public TsdbResult[] evaluate(List<TsdbResult[]> queryResults) {
		List<TsdbResult[]> materialized = Lists.newArrayList();
		List<Integer> metricQueryKeys = null;
		if (subMetricQueries != null && subMetricQueries.size() > 0) {
			metricQueryKeys = Lists.newArrayList(subMetricQueries.keySet());
			Collections.sort(metricQueryKeys);
		}

		int metricPointer = 0;
		int subExprPointer = 0;
		for (int i = 0; i < parameterSourceIndex.size(); i++) {
			Parameter p = parameterSourceIndex.get(i);

			if (p == Parameter.METRIC_QUERY) {
				if (metricQueryKeys == null) {
					throw new RuntimeException("Attempt to read metric " +
							"results when none exist");
				}

				int ix = metricQueryKeys.get(metricPointer++);
				materialized.add(queryResults.get(ix));
			} else if (p == Parameter.SUB_EXPRESSION) {
				ExpressionTree st = subExpressions.get(subExprPointer++);
				materialized.add(st.evaluate(queryResults));
			} else {
				throw new RuntimeException("Unknown value: " + p);
			}
		}

		return expr.evaluate(dataQuery, materialized, funcParams);
	}

	public String toString() {
		return writeStringField();
	}

	public String writeStringField() {
		List<String> strs = Lists.newArrayList();
		if (subExpressions != null) {
			for (ExpressionTree sub : subExpressions) {
				strs.add(sub.toString());
			}
		}

		if (subMetricQueries != null) {
			String subMetrics = clean(subMetricQueries.values());
			if (subMetrics != null && subMetrics.length() > 0) {
				strs.add(subMetrics);
			}
		}

		String innerExpression = DOUBLE_COMMA_JOINER.join(strs);
		return expr.writeStringField(funcParams, innerExpression);
	}

	private String clean(Collection<String> values) {
		if (values == null || values.size() == 0) {
			return "";
		}

		List<String> strs = Lists.newArrayList();
		for (String v : values) {
			String tmp = v.replaceAll("\\{.*\\}", "");
			int ix = tmp.lastIndexOf(':');
			if (ix < 0) {
				strs.add(tmp);
			} else {
				strs.add(tmp.substring(ix + 1));
			}
		}

		return DOUBLE_COMMA_JOINER.join(strs);
	}

}