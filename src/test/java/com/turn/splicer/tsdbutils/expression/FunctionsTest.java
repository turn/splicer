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

import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.SplicerQueryRunner;
import com.turn.splicer.tsdbutils.SplicerUtils;
import com.turn.splicer.tsdbutils.TsQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * Created by bpeltz on 9/15/15.
 */
public class FunctionsTest {

	final TsQuery dataQuery = new TsQuery();

	@Mock
	private SplicerQueryRunner queryRunner;

	@InjectMocks
	private ExpressionTree expressionTree;

	/**
	 * call before initMocks() is called so that I can inject the mocked SplicerQueryRunner
	 * into expressionTree after its created (I think that is the right way to do this)
	 */
	private void setupExpressionTree(String query) {
		setupExpressionTree(query, "0", "0");
	}

	private void setupExpressionTree(String query, String start, String end) {
		List<ExpressionTree> expressionTrees = new ArrayList<ExpressionTree>();
		String[] expressions = {query};
		List<String> metricQueries = new ArrayList<String>();
		dataQuery.setStart(start);
		dataQuery.setEnd(end);

		SplicerUtils.syntaxCheck(expressions, dataQuery, metricQueries, expressionTrees);
		expressionTree = expressionTrees.get(0);
	}

	private TsdbResult getDefaultQueryResult() {
		TsdbResult defaultResult = new TsdbResult();

		defaultResult.setMetric("tcollector.collector.lines_received");
		Map<String, Object> dps = new HashMap<String, Object>();
		dps.put("1", 10.0);
		dps.put("2", 20.0);
		dps.put("3", 30.0);

		defaultResult.setTags(new TsdbResult.Tags(new HashMap<String, String>()));

		TsdbResult.Points points = new TsdbResult.Points(dps);
		defaultResult.setDps(points);

		return defaultResult;
	}

	private TsdbResult[] getDefaultQueryResultArray() {
		return new TsdbResult[]{ getDefaultQueryResult() };
	}

	private TsdbResult[] getCorrectTimeShiftResult() {
		TsdbResult correctResult = new TsdbResult();

		correctResult.setMetric("tcollector.collector.lines_received");
		Map<String, Object> dps = new HashMap<String, Object>();
		dps.put("2", 10.0);
		dps.put("3", 20.0);
		dps.put("4", 30.0);

		correctResult.setTags(new TsdbResult.Tags(new HashMap<String, String>()));

		TsdbResult.Points points = new TsdbResult.Points(dps);
		correctResult.setDps(points);

		TsdbResult[] finalResult = {correctResult};
		return finalResult;
	}

	private TsdbResult[] getCorrectResult() {
		TsdbResult correctResult = new TsdbResult();

		correctResult.setMetric("tcollector.collector.lines_received");
		Map<String, Object> dps = new HashMap<String, Object>();
		dps.put("1", 100.0);
		dps.put("2", 200.0);
		dps.put("3", 300.0);

		correctResult.setTags(new TsdbResult.Tags(new HashMap<String, String>()));

		TsdbResult.Points points = new TsdbResult.Points(dps);
		correctResult.setDps(points);

		TsdbResult[] finalResult = {correctResult};
		return finalResult;
	}

	/**
	 * builds an ExpressionTree off of query param, evaluates that expression tree
	 * mocking results from Tsdb. Instead of calling tsdb in sliceAndRunQuery
	 * it will just return mockQueryResult in all cases. toCompare is the result
	 * to test equality against your expressions evaluation.
	 * @param query
	 * @param mockQueryResult
	 * @param toCompare
	 * @param outcome
	 * @throws IOException
	 */
	private void evaluateQuery(String query, TsdbResult mockQueryResult, TsdbResult[] toCompare, boolean outcome) throws IOException, ExecutionException, InterruptedException {
		evaluateQuery(query, mockQueryResult, toCompare, outcome, null, null);
	}

	private void evaluateQuery(String query, TsdbResult mockQueryResult, TsdbResult[] toCompare, boolean outcome, String start, String end) throws IOException, ExecutionException, InterruptedException {
		setupExpressionTree(query, start, end);
		MockitoAnnotations.initMocks(this);

		TsdbResult[] mockQueryResultArray = new TsdbResult[1];
		mockQueryResultArray[0] = mockQueryResult;

		when(queryRunner.sliceAndRunQuery(any(TsQuery.class), any(RegionChecker.class))).thenReturn(mockQueryResultArray);

		TsdbResult[] tsdbResults = null;

		tsdbResults = expressionTree.evaluateAll();

		for(TsdbResult ts: tsdbResults) {
			System.out.println("res: " + ts.toString());
		}

		for(TsdbResult tc: toCompare) {
			System.out.println("tc: " + tc.toString());
		}
		assertEquals(Arrays.deepEquals(tsdbResults, toCompare), outcome);
		assertEquals(Arrays.deepEquals(toCompare, tsdbResults), outcome);
	}

		@Test
	public void evaluateScaleSimple() throws IOException, ExecutionException, InterruptedException {
		evaluateQuery("scale(sum:1m-avg:tcollector.collector.lines_received,10)", getDefaultQueryResult(), getCorrectResult(), true, "1000", "1001");
	}

	@Test void evaluateAliasSimple() throws IOException, ExecutionException, InterruptedException {
		TsdbResult correctResult = getDefaultQueryResult();

		TsdbResult[] correctResultArray = new TsdbResult[]{correctResult};
		evaluateQuery("alias(sum:1m-avg:tcollector.collector.lines_received,dumpy)", getDefaultQueryResult(), correctResultArray, false, "1000", "1001");

		correctResult.setAlias("dumpy");
		evaluateQuery("alias(sum:1m-avg:tcollector.collector.lines_received,dumpy)", getDefaultQueryResult(), correctResultArray, true, "1000", "1001");

	}

	@Test
	public void timeShiftFunctionSimple() throws IOException, ExecutionException, InterruptedException {
		//start end time given in millis
		evaluateQuery("timeShift(sum:1m-avg:tcollector.collector.lines_received,'1sec')", getDefaultQueryResult(), getCorrectTimeShiftResult(), true, "2000", "4000");
	}
}
