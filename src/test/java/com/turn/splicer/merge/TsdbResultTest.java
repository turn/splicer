package com.turn.splicer.merge;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.internal.Lists;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TsdbResultTest {

	@Test
	public void createTsdbResult() {
		TsdbResult obj = new TsdbResult();
		obj.setMetric("metric.name");
		obj.setAggregateTags(Arrays.asList("host", "domain"));
	}

	@Test
	public void deserializeTest() throws Exception
	{
		TsdbResult.JSON_MAPPER.readValue("{\"1438383600\":8499332.134660648, \"1438383601\":84}",
				TsdbResult.Points.class);

		TsdbResult.JSON_MAPPER.readValue("{\"metric\": \"name\"}}", TsdbResult.class);

		TsdbResult.JSON_MAPPER.readValue("{\"metric\": \"name\", \"dps\": {\"1438383600\":"
				+ "8499332.134660648, \"1438383601\":84}}", TsdbResult.class);

		String js = "{\"metric\":\"turn.bid.rtb.bidrequest.bid\",\"tags\":{\"invsource\":\"adx\"}"
				+ ",\"aggregateTags\":[\"serverid\",\"servertype\",\"host\",\"seat\""
				+ ",\"machine_class\",\"mediaChannelId\",\"domain\"],"
				+ "\"dps\":{\"1438383600\":8499332.134660648,\"1438383660\":5189956.492815415,"
				+ "\"1438383720\":3625048.848030829,\"1438383780\":2767397.69178366,"
				+ "\"1438383840\":2259138.068511685,\"1438383900\":2259310.6599630998,"
				+ "\"1438383960\":2259482.384829848,\"1438384020\":2259648.478802788,"
				+ "\"1438384080\":2259814}}";

		TsdbResult r = TsdbResult.from(js);

		Assert.assertEquals(r.getMetric(), "turn.bid.rtb.bidrequest.bid");
		List<String> aggTagsExp = Lists.newArrayList();
		aggTagsExp.add("serverid");
		aggTagsExp.add("servertype");
		aggTagsExp.add("host");
		aggTagsExp.add("seat");
		aggTagsExp.add("machine_class");
		aggTagsExp.add("mediaChannelId");
		aggTagsExp.add("domain");
		Assert.assertTrue(r.getAggregateTags().containsAll(aggTagsExp));

		Map<String, String> tags = r.getTags().getTags();
		Assert.assertEquals(tags.size(), 1);
		Assert.assertEquals(tags.get("invsource"), "adx");

		Assert.assertEquals(r.getDps().getMap().get("1438383660"), 5189956.492815415);
		Assert.assertEquals(r.getDps().getMap().get("1438383720"), 3625048.848030829);
		Assert.assertEquals(r.getDps().getMap().get("1438383960"), 2259482.384829848);
		Assert.assertEquals(r.getDps().getMap().get("1438384020"), 2259648.478802788);
	}

	@Test
	public void deserializeArray() throws IOException
	{
		String j = "[{\"metric\":\"turn.bid.rtb.bidrequest.bid\",\"tags\":{\"invsource\":\"adx\"}"
				+ ",\"aggregateTags\":[\"serverid\",\"servertype\",\"host\",\"seat\""
				+ ",\"machine_class\",\"mediaChannelId\",\"domain\"],"
				+ "\"dps\":{\"1438383600\":8499332.134660648,\"1438383660\":5189956.492815415,"
				+ "\"1438383720\":3625048.848030829,\"1438383780\":2767397.69178366,"
				+ "\"1438383840\":2259138.068511685,\"1438383900\":2259310.6599630998,"
				+ "\"1438383960\":2259482.384829848,\"1438384020\":2259648.478802788,"
				+ "\"1438384080\":2259814}}]";

		TsdbResult[] results = TsdbResult.JSON_MAPPER.readValue(j, TsdbResult[].class);
		Assert.assertEquals(results.length, 1);

		TsdbResult r = results[0];

		Assert.assertEquals(r.getMetric(), "turn.bid.rtb.bidrequest.bid");
		List<String> aggTagsExp = Lists.newArrayList();
		aggTagsExp.add("serverid");
		aggTagsExp.add("servertype");
		aggTagsExp.add("host");
		aggTagsExp.add("seat");
		aggTagsExp.add("machine_class");
		aggTagsExp.add("mediaChannelId");
		aggTagsExp.add("domain");
		Assert.assertTrue(r.getAggregateTags().containsAll(aggTagsExp));

		Map<String, String> tags = r.getTags().getTags();
		Assert.assertEquals(tags.size(), 1);
		Assert.assertEquals(tags.get("invsource"), "adx");

		Assert.assertEquals(r.getDps().getMap().get("1438383660"), 5189956.492815415);
		Assert.assertEquals(r.getDps().getMap().get("1438383720"), 3625048.848030829);
		Assert.assertEquals(r.getDps().getMap().get("1438383960"), 2259482.384829848);
		Assert.assertEquals(r.getDps().getMap().get("1438384020"), 2259648.478802788);

	}

}
