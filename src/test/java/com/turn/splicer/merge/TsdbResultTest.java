package com.turn.splicer.merge;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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

	@Test
	public void testEquals() {
		TsdbResult result1 = new TsdbResult();
		TsdbResult result2 = null;

		Assert.assertFalse(result1.equals(result2));

		result2 = new TsdbResult();
		Assert.assertTrue(result1.equals(result2));

		result1.setMetric("dummy");
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		result2.setMetric("not_dummy");
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		result2.setMetric("dummy");
		Assert.assertTrue(result1.equals(result2));
		Assert.assertTrue(result2.equals(result1));

		result1.setAlias("alias_dummy");
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		result2.setAlias("not_alias_dummy");
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		result2.setAlias("alias_dummy");
		Assert.assertTrue(result1.equals(result2));
		Assert.assertTrue(result2.equals(result1));

		Map<String, String> result1tags = new HashMap<String, String>();
		result1.setTags(new TsdbResult.Tags(result1tags));
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		result1tags.put("key1", "value1");
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		Map<String, String> result2tags = new HashMap<String, String>();
		result2.setTags(new TsdbResult.Tags(result2tags));
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		result2tags.put("key1", "value1");
		Assert.assertTrue(result1.equals(result2));
		Assert.assertTrue(result2.equals(result1));

		Map<String, Object> result1map = new HashMap<String, Object>();
		result1.setDps(new TsdbResult.Points(result1map));
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		Map<String, Object> result2map = new HashMap<String, Object>();
		result2.setDps(new TsdbResult.Points(result2map));
		Assert.assertTrue(result1.equals(result2));
		Assert.assertTrue(result2.equals(result1));

		result1map.put("point1", "point1val");
		Assert.assertFalse(result1.equals(result2));
		Assert.assertFalse(result2.equals(result1));

		result2map.put("point1", "point1val");
		Assert.assertTrue(result1.equals(result2));
		Assert.assertTrue(result2.equals(result1));
	}

}
