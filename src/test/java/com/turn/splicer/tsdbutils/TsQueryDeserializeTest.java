package com.turn.splicer.tsdbutils;

import java.util.ArrayList;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TsQueryDeserializeTest {

	@Test(expectedExceptions = RuntimeException.class)
	public void tryEmptyPostDeserialization() {
		TsQuerySerializer.deserializeFromJson("");
	}

	@Test(expectedExceptions = NullPointerException.class)
	public void tryNullPostDeserialization() {
		TsQuerySerializer.deserializeFromJson(null);
	}

	@Test
	public void tryRealData() {
		String jsonContent =
				"{\"" +
						"start\":1438377954073,\"" +
						"queries\":" +
						"[" +
							"{\"metric\":\"tsd.jvm.ramused\"," +
							"\"aggregator\":\"avg\"," +
							"\"downsample\":\"1m-avg\"," +
							"\"tags\":" +
							"{\"host\":\"dwh-edge003\"}}" +
						"]}";
		TsQuery query = TsQuerySerializer.deserializeFromJson(jsonContent);

		Assert.assertTrue(query.getStart().equals("1438377954073"));
		Assert.assertNotNull(query.getQueries());
		Assert.assertEquals(query.getQueries().size(), 1);

		TSSubQuery subQuery = query.getQueries().get(0);
		Assert.assertEquals(subQuery.getMetric(), "tsd.jvm.ramused");
		Assert.assertEquals(subQuery.getAggregator(), "avg");
		Assert.assertEquals(subQuery.getDownsample(), "1m-avg");
		Assert.assertEquals(subQuery.getTags().get("host"), "dwh-edge003");
		Assert.assertEquals(subQuery.getTags().size(), 1);
	}

	@Test
	public void deserializeAndValidate() {
		String jsonContent =
				"{\"" +
						"start\":1438377954073,\"" +
						"queries\":" +
						"[" +
						"{\"metric\":\"tsd.jvm.ramused\"," +
						"\"aggregator\":\"avg\"," +
						"\"downsample\":\"1m-avg\"," +
						"\"tags\":" +
						"{\"host\":\"dwh-edge003\"}}" +
						"]}";
		TsQuery query = TsQuerySerializer.deserializeFromJson(jsonContent);
		query.validateAndSetQuery();

		Assert.assertEquals(query.startTime(), 1438377954073L);
		// end time time should be approximately equal to current time.
		Assert.assertTrue((System.currentTimeMillis() - query.endTime()) < 1000);
	}

	@Test
	public void serialize() {
		TsQuery query = new TsQuery();
		query.setStart(String.valueOf(System.currentTimeMillis() - 7200000)); //2 hours
		query.setEnd(String.valueOf(System.currentTimeMillis()));
		TSSubQuery subQuery = new TSSubQuery();
		subQuery.setDownsample("10m-avg");
		subQuery.setAggregator("sum");
		subQuery.setMetric("tsd.queries");
		subQuery.setRate(true);
		query.setQueries(new ArrayList<>(Collections.singleton(subQuery)));
	}

	@Test
	public void serializeAndDeserialize() {
		String jsonContent =
				"{\"" +
						"start\":1438377954073,\"" +
						"queries\":" +
						"[" +
						"{\"metric\":\"tsd.jvm.ramused\"," +
						"\"aggregator\":\"avg\"," +
						"\"downsample\":\"1m-avg\"," +
						"\"tags\":" +
						"{\"host\":\"dwh-edge003\"}}" +
						"]}";
		TsQuery query = TsQuerySerializer.deserializeFromJson(jsonContent);
		query.validateAndSetQuery();

		long current = System.currentTimeMillis();
		TsQuery slice = TsQuery.sliceOf(query, current - 3600, current);
		Assert.assertEquals(slice.startTime(), current - 3600);
	}

}
