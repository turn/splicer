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

package com.turn.splicer.tsdbutils;

import com.turn.splicer.merge.TsdbResult;
import net.opentsdb.core.*;
import com.turn.splicer.tsdbutils.expression.EndpointAligningAggregationIterator;
import com.turn.splicer.tsdbutils.expression.Expression;
import com.turn.splicer.tsdbutils.expression.SeekableViewDataPointImpl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Functions {

	private static final Logger logger = LoggerFactory.getLogger(Functions.class);

	private static enum MaxExpressionType {
		CURRENT, MAX
	}

	public static class TimeShiftFunction implements Expression {


		/**
		 * in place modify of TsdbResult array to increase timestamps by timeshift
		 * @param dataQuery
		 * @param queryResults
		 * @param queryParams
		 * @return
		 */
		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> queryParams) {
			//not 100% sure what to do here -> do I need to think of the case where I have no data points
			if(queryResults == null || queryResults.isEmpty()) {
				return new TsdbResult[]{};
			}

			if(queryParams == null || queryResults.isEmpty()) {
				throw new NullPointerException("Need amount of timeshift to perform timeshift");
			}

			String param = queryParams.get(0);
			if (param == null || param.length() == 0) {
				throw new NullPointerException("Invalid timeshift='" + param + "'");
			}

			param = param.trim();

			long timeshift = -1;
			if (param.startsWith("'") && param.endsWith("'")) {
				timeshift = parseParam(param) / 1000;
			} else {
				throw new RuntimeException("Invalid timeshift parameter: eg '10min'");
			}

			if (timeshift <= 0) {
				throw new RuntimeException("timeshift <= 0");
			}

			TsdbResult[] inputPoints = queryResults.get(0);

			for(TsdbResult input: inputPoints) {
				Map<String, Object> inputMap = input.getDps().getMap();
				Map<String, Object> outputMap = new HashMap<String, Object>();
				for (String oldKey : inputMap.keySet()) {
					Long newtime = Long.parseLong(oldKey) + (Long) timeshift;
					String newKey = Long.toString(newtime);
					Object val = inputMap.get(oldKey);
					outputMap.put(newKey, val);
				}
				input.setDps(new TsdbResult.Points(outputMap));
			}

			return inputPoints;
		}

		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return null;
		}
	}

	public static class MovingAverageFunction implements Expression {

		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> params) {

			//TODO: Why can MovingAverageFunction return an empty set

			if (queryResults == null || queryResults.isEmpty()) {
				return new TsdbResult[]{};
			}

			if (params == null || params.isEmpty()) {
				throw new NullPointerException("Need aggregation window for moving average");
			}

			String param = params.get(0);
			if (param == null || param.length() == 0) {
				throw new NullPointerException("Invalid window='" + param + "'");
			}

			param = param.trim();

			long numPoints = -1;
			boolean isTimeUnit = false;
			if (param.matches("[0-9]+")) {
				numPoints = Integer.parseInt(param);
			} else if (param.startsWith("'") && param.endsWith("'")) {
				numPoints = parseParam(param);
				isTimeUnit = true;
			}

			if (numPoints <= 0) {
				throw new RuntimeException("numPoints <= 0");
			}

			int size = 0;
			for (TsdbResult[] results : queryResults) {
				size = size + results.length;
			}

			SeekableView[] views = new SeekableView[size];
			int index = 0;

			for (TsdbResult[] resultArray : queryResults) {
				for (TsdbResult oneResult : resultArray) {
					try {
						views[index] = new SeekableViewDataPointImpl(oneResult.getDps().getDataPointsFromTreeMap());
					} catch (Exception e) {
						e.printStackTrace();
					}
					index++;
				}
			}

			SeekableView view = new AggregationIterator(views,
					dataQuery.startTime() / 1000, dataQuery.endTime() / 1000,
					new Aggregators.MovingAverage(Aggregators.Interpolation.LERP, "movingAverage", numPoints, isTimeUnit),
					Aggregators.Interpolation.LERP, false);

			TsdbResult singleResult;

			if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
				singleResult = TsdbResult.copyMeta(queryResults.get(0)[0]);
				while (view.hasNext()) {
					singleResult.getDps().addPoint(view.next());
				}
			} else {
				singleResult = new TsdbResult();
			}


			TsdbResult[] finalResult = new TsdbResult[1];
			finalResult[0] = singleResult;

			return finalResult;


		}


		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "movingAverage(" + innerExpression + ")";
		}

	}

	public static class DivideSeriesFunction implements Expression {

		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> params) {
			if (queryResults == null || queryResults.isEmpty()) {
				throw new NullPointerException("Query results cannot be empty");
			}

			//we'll end up with x + -y
			TsdbResult x, y;
			if (queryResults.size() == 2 && queryResults.get(0).length == 1
					&& queryResults.get(1).length == 1) {
				x = queryResults.get(0)[0];
				y = queryResults.get(1)[0];
			} else if (queryResults.size() == 1 && queryResults.get(0).length == 2) {
				x = queryResults.get(0)[0];
				y = queryResults.get(0)[1];
			} else {
				throw new RuntimeException("Expected two query results for difference");
			}

			int size = 2;
			SeekableView[] views = new SeekableView[size];

			//now add x to views

			try {
				views[0] = new SeekableViewDataPointImpl(x.getDps().getDataPointsFromTreeMap());
				views[1] = new SeekableViewDataPointImpl(y.getDps().getDataPointsFromTreeMapReciprocal());
			} catch (Exception e) {
				logger.error("Could not create view", e);
			}

			SeekableView view = (new EndpointAligningAggregationIterator(views,
					dataQuery.startTime() / 1000, dataQuery.endTime() / 1000,
					Aggregators.MULTIPLY, Aggregators.Interpolation.LERP, false));

			TsdbResult singleResult;

			if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
				singleResult = TsdbResult.copyMeta(queryResults.get(0)[0]);
				while(view.hasNext()) {
					singleResult.getDps().addPoint(view.next());
				}
			} else {
				singleResult = new TsdbResult();
			}


			TsdbResult[] finalResult = new TsdbResult[1];
			finalResult[0] = singleResult;

			return finalResult;
		}


		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "divideSeries(" + innerExpression + ")";
		}
	}

	public static class MultiplySeriesFunction implements Expression {

		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> queryParams) {
			if (queryResults == null || queryResults.isEmpty()) {
				throw new NullPointerException("Query results cannot be empty");
			}

			int size = 0;
			for(TsdbResult[] queryResult: queryResults) {
				size = size + queryResult.length;
			}

			SeekableView[] views = new SeekableView[size];
			int index = 0;
			for(TsdbResult[] resultArray: queryResults) {
				for(TsdbResult oneResult: resultArray) {
					try {
						views[index] = new SeekableViewDataPointImpl(oneResult.getDps().getDataPointsFromTreeMap());
					} catch (Exception e) {
						e.printStackTrace();
					}
					index++;
				}
			}

			SeekableView view = (new AggregationIterator(views,
					dataQuery.startTime() / 1000, dataQuery.endTime() / 1000,
					Aggregators.MULTIPLY, Aggregators.Interpolation.LERP, false));

			TsdbResult singleResult;

			if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
				singleResult = TsdbResult.copyMeta(queryResults.get(0)[0]);
				while(view.hasNext()) {
					singleResult.getDps().addPoint(view.next());
				}
			} else {
				singleResult = new TsdbResult();
			}


			TsdbResult[] finalResult = new TsdbResult[1];
			finalResult[0] = singleResult;

			return finalResult;
		}

		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "multiplySeries(" + innerExpression + ")";
		}
	}

	public static class DifferenceSeriesFunction implements Expression {

		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> params) {
			if (queryResults == null || queryResults.isEmpty()) {
				throw new NullPointerException("Query results cannot be empty");
			}

			//we'll end up with x + -y
			TsdbResult x, y;
			if (queryResults.size() == 2 && queryResults.get(0).length == 1
					&& queryResults.get(1).length == 1) {
				x = queryResults.get(0)[0];
				y = queryResults.get(1)[0];
			} else if (queryResults.size() == 1 && queryResults.get(0).length == 2) {
				x = queryResults.get(0)[0];
				y = queryResults.get(0)[1];
			} else {
				throw new RuntimeException("Expected two query results for difference");
			}

			int size = 2;
			SeekableView[] views = new SeekableView[size];

			//now add x to views

			try {
				views[0] = new SeekableViewDataPointImpl(x.getDps().getDataPointsFromTreeMap());
				views[1] = new SeekableViewDataPointImpl(y.getDps().getDataPointsFromTreeMap(-1));
			} catch (Exception e) {
				e.printStackTrace();
			}

			SeekableView view = (new EndpointAligningAggregationIterator(views,
					dataQuery.startTime() / 1000, dataQuery.endTime() / 1000,
					Aggregators.SUM, Aggregators.Interpolation.LERP, false));

			TsdbResult singleResult;

			if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
				singleResult = TsdbResult.copyMeta(queryResults.get(0)[0]);
				while(view.hasNext()) {
					singleResult.getDps().addPoint(view.next());
				}
			} else {
				singleResult = new TsdbResult();
			}


			TsdbResult[] finalResult = new TsdbResult[1];
			finalResult[0] = singleResult;

			return finalResult;
		}

		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "differenceSeries(" + innerExpression + ")";
		}
	}

	public static class SumSeriesFunction implements Expression {

		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> params) {
			if (queryResults == null || queryResults.isEmpty()) {
				throw new NullPointerException("Query results cannot be empty");
			}

			int size = 0;
			for(TsdbResult[] queryResult: queryResults) {
				size = size + queryResult.length;
			}

			SeekableView[] views = new SeekableView[size];
			int index = 0;
			for(TsdbResult[] resultArray: queryResults) {
				for(TsdbResult oneResult: resultArray) {
					try {
						views[index] = new SeekableViewDataPointImpl(oneResult.getDps().getDataPointsFromTreeMap());
					} catch (Exception e) {
						e.printStackTrace();
					}
					index++;
				}
			}

			SeekableView view = (new AggregationIterator(views,
					dataQuery.startTime() / 1000, dataQuery.endTime() / 1000,
					Aggregators.SUM, Aggregators.Interpolation.LERP, false));

			//Ok now I just need to make the AggregationIterator spit out
			//Map elements or convert them to data points?

			TsdbResult singleResult;

			if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
				singleResult = TsdbResult.copyMeta(queryResults.get(0)[0]);
				while(view.hasNext()) {
					singleResult.getDps().addPoint(view.next());
				}
			} else {
				singleResult = new TsdbResult();
			}


			TsdbResult[] finalResult = new TsdbResult[1];
			finalResult[0] = singleResult;

			return finalResult;
		}

		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "sumSeries(" + innerExpression + ")";
		}
	}

	public static class AbsoluteValueFunction implements Expression {

		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> params) {
			if (queryResults == null || queryResults.isEmpty()) {
				throw new NullPointerException("Query results cannot be empty");
			}

			TsdbResult[] inputPoints = queryResults.get(0);

			for (int i = 0; i < inputPoints.length; i++) {
				absoluteValue(inputPoints[i]);
			}

			return inputPoints;
		}

		private TsdbResult absoluteValue(TsdbResult input) {
			Map<String, Object> inputMap = input.getDps().getMap();
			for(String key : inputMap.keySet()) {
				Object val = inputMap.get(key);
				if(val instanceof Double) {
					double value = Math.abs((double) val);
					inputMap.put(key, value);
				} else if (val instanceof Long) {
					long value = Math.abs((long) val);
					inputMap.put(key, value);
				} else {
					throw new RuntimeException("Expected type Long or Double instead found: "
							+ val.getClass());
				}
			}

			return input;
		}


		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "abs(" + innerExpression + ")";
		}
	}

	public static class ScaleFunction implements Expression {

		@Override
		public TsdbResult[] evaluate(TsQuery dataQuery, List<TsdbResult[]> queryResults, List<String> params) {
			if (queryResults == null || queryResults.isEmpty()) {
				throw new NullPointerException("Query results cannot be empty");
			}

			if (params == null || params.isEmpty()) {
				throw new NullPointerException("Scaling parameter not available");
			}

			String factor = params.get(0);
			factor = factor.replaceAll("'|\"", "").trim();
			double scaleFactor = Double.parseDouble(factor);

			TsdbResult[] inputPoints = queryResults.get(0);

			for (int i = 0; i < inputPoints.length; i++) {
				scale(inputPoints[i], scaleFactor);
			}

			return inputPoints;
		}

		private TsdbResult scale(TsdbResult input, double scaleFactor) {
			//now iterate over all points in the input map and add them to output

			Map<String, Object> inputMap = input.getDps().getMap();
			for (String key : inputMap.keySet()) {
				Object val = inputMap.get(key);
				if (val instanceof Double) {
					inputMap.put(key, new Double(((Double) val).doubleValue() * scaleFactor));
				} else if (val instanceof Long) {
					inputMap.put(key, new Long(((Long) val).longValue() * (long) scaleFactor));
				} else {
					//throw an exception
					throw new RuntimeException("Expected type Long or Double instead found: "
							+ val.getClass());
				}
			}

			return input;
		}

		@Override
		public String writeStringField(List<String> queryParams, String innerExpression) {
			return "scale(" + innerExpression + ")";
		}
	}

	public static long parseParam(String param) {
		char[] chars = param.toCharArray();
		int tuIndex = 0;
		for (int c = 1; c < chars.length; c++) {
			if (Character.isDigit(chars[c])) {
				tuIndex++;
			} else {
				break;
			}
		}

		if (tuIndex == 0) {
			throw new RuntimeException("Invalid Parameter: " + param);
		}

		int time = Integer.parseInt(param.substring(1, tuIndex + 1));
		String unit = param.substring(tuIndex + 1, param.length() - 1);
		if ("sec".equals(unit)) {
			return TimeUnit.MILLISECONDS.convert(time, TimeUnit.SECONDS);
		} else if ("min".equals(unit)) {
			return TimeUnit.MILLISECONDS.convert(time, TimeUnit.MINUTES);
		} else if ("hr".equals(unit)) {
			return TimeUnit.MILLISECONDS.convert(time, TimeUnit.HOURS);
		} else if ("day".equals(unit) || "days".equals(unit)) {
			return TimeUnit.MILLISECONDS.convert(time, TimeUnit.DAYS);
		} else if ("week".equals(unit) || "weeks".equals(unit)) {
			//didn't have week so small cheat here
			return TimeUnit.MILLISECONDS.convert(time*7, TimeUnit.DAYS);
		}
		else {
			throw new RuntimeException("unknown time unit=" + unit);
		}
	}
}
