/**
 * Copyright 2015-2016 The Splicer Query Engine Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.turn.splicer.tsdbutils.expression;

import com.turn.splicer.SplicerServlet;
import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.merge.TsdbResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.turn.splicer.tsdbutils.Functions;
import com.turn.splicer.tsdbutils.SplicerQueryRunner;
import com.turn.splicer.tsdbutils.SplicerUtils;
import com.turn.splicer.tsdbutils.TsQuery;

public class ExpressionTree {

	private final Expression expr;
	private final TsQuery dataQuery;

	private List<ExpressionTree> subExpressions;
	private List<String> funcParams;
	private Map<Integer, String> subMetricQueries;
	private Map<Integer, Parameter> parameterSourceIndex = Maps.newHashMap();

	private static final Joiner DOUBLE_COMMA_JOINER = Joiner.on(",").skipNulls();

	private SplicerQueryRunner queryRunner;

	private static ExecutorService pool = Executors.newCachedThreadPool();


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
		this.queryRunner = new SplicerQueryRunner();
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

	public TsdbResult[] evaluateAll() throws ExecutionException, InterruptedException {

		List<Integer> metricQueryKeys = null;

		if (subMetricQueries != null && subMetricQueries.size() > 0) {
			metricQueryKeys = Lists.newArrayList(subMetricQueries.keySet());
			Collections.sort(metricQueryKeys);
		}

		int metricPointer = 0;
		int subExprPointer = 0;

		if(expr instanceof Functions.TimeShiftFunction) {
			String param = funcParams.get(0);
			if (param == null || param.length() == 0) {
				throw new NullPointerException("Invalid timeshift='" + param + "'");
			}

			param = param.trim();

			long timeshift = -1;
			if (param.startsWith("'") && param.endsWith("'")) {
				timeshift = Functions.parseParam(param);
			} else {
				throw new RuntimeException("Invalid timeshift parameter: eg '10min'");
			}

			long newStart = dataQuery.startTime() - timeshift;
			long oldStart = dataQuery.startTime();
			dataQuery.setStart(Long.toString(newStart));
			long newEnd = dataQuery.endTime() - timeshift;
			long oldEnd = dataQuery.endTime();
			dataQuery.setEnd(Long.toString(newEnd));
			dataQuery.validateTimes();
		}

		List<Future<TsdbResult[]>> tsdbResultFutures = new ArrayList(parameterSourceIndex.size());

		for (int i = 0; i < parameterSourceIndex.size(); i++) {
			Parameter p = parameterSourceIndex.get(i);

			if (p == Parameter.METRIC_QUERY) {
				if (metricQueryKeys == null) {
					throw new RuntimeException("Attempt to read metric " +
							"results when none exists");
				}

				int ix = metricQueryKeys.get(metricPointer++);
				String query = subMetricQueries.get(ix);

				TsQuery realQuery = TsQuery.validCopyOf(dataQuery);

				SplicerUtils.parseMTypeSubQuery(query, realQuery);

				realQuery.validateAndSetQuery();
				RegionChecker checker = SplicerServlet.REGION_UTIL.getRegionChecker();

				tsdbResultFutures.add(pool.submit(new QueryRunnerWorker(queryRunner, realQuery, checker)));

			} else if (p == Parameter.SUB_EXPRESSION) {
				ExpressionTree nextExpression = subExpressions.get(subExprPointer++);
				tsdbResultFutures.add(pool.submit(new ExpressionTreeWorker(nextExpression)));
			} else {
				throw new RuntimeException("Unknown value: " + p);
			}
		}

		List<TsdbResult[]> orderedSubResults = Lists.newArrayList();

		for (Future<TsdbResult[]> tsdbResultFuture : tsdbResultFutures) {
			orderedSubResults.add(tsdbResultFuture.get());
		}

		return expr.evaluate(dataQuery, orderedSubResults, funcParams);
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