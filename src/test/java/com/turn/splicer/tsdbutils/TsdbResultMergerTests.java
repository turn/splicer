package com.turn.splicer.tsdbutils;

import com.turn.splicer.merge.MergeException;
import com.turn.splicer.merge.ResultsMerger;
import com.turn.splicer.merge.TsdbResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class TsdbResultMergerTests {

	ResultsMerger merger = new ResultsMerger();

	@Test
	public void testSerialize() throws IOException
	{
		TsdbResult obj1 = get("a.b.c");
		addTags(obj1, "key", "val");
		obj1.setAggregateTags(Arrays.asList("x", "y"));
		addFixedPoints(obj1);

		TsdbResult obj2 = get("a.b.c");
		addTags(obj2, "key", "val");
		obj2.setAggregateTags(Arrays.asList("u", "v"));
		addFixedPoints(obj2);

		String t = TsdbResult.toJson(new TsdbResult[]{obj1, obj2});
		Assert.assertNotNull(t);
		Assert.assertTrue(t.length() > 0);

		TsdbResult[] arr = TsdbResult.fromArray(t);
		Assert.assertNotNull(arr);
		Assert.assertTrue(arr.length == 2);

		Assert.assertEquals(arr[0].getMetric(), "a.b.c");
		Assert.assertEquals(arr[1].getMetric(), "a.b.c");

		Assert.assertTrue(arr[0].getAggregateTags().containsAll(Arrays.asList("x", "y")));
		Assert.assertTrue(arr[1].getAggregateTags().containsAll(Arrays.asList("u", "v")));

		for (TsdbResult result: arr) {
			Assert.assertEquals(result.getDps().getMap().get("1234500000"), 11L);
			Assert.assertEquals(result.getDps().getMap().get("1234501000"), 12L);
			Assert.assertEquals(result.getDps().getMap().get("1234502000"), 13L);
			Assert.assertEquals(result.getDps().getMap().get("1234503000"), 14L);
		}
	}

	@Test
	public void testMergeSameResult()
	{
		TsdbResult obj1 = get("a.b.c");
		addFixedPoints(obj1);
		TsdbResult obj2 = get("a.b.c");
		addFixedPoints(obj2);

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);

		TsdbResult result = results[0];
		Assert.assertEquals(result.getMetric(), "a.b.c");

		Assert.assertEquals(result.getDps().getMap().size(), 4);

		Assert.assertEquals(result.getDps().getMap().get("1234500000"), 11);
		Assert.assertEquals(result.getDps().getMap().get("1234501000"), 12);
		Assert.assertEquals(result.getDps().getMap().get("1234502000"), 13);
		Assert.assertEquals(result.getDps().getMap().get("1234503000"), 14);
	}

	@Test
	public void mergeMultiTagResults()
	{
		TsdbResult[] first = new TsdbResult[4];
		TsdbResult[] second = new TsdbResult[4];

		for (int i=0; i<first.length; i++) {
			TsdbResult t = first[i] = get("a.b.c");
			t.setAggregateTags(Arrays.asList("a", "b"));
			addTags(t, "tag", "dom" + i);
			for (int j=0; j < 10; j++) {
				putPoint(t, 1000 + i * 100 + j, 1000 + i * 100 + j);
			}
		}

		for (int i=0; i<second.length; i++) {
			TsdbResult t = second[i] = get("a.b.c");
			t.setMetric("a.b.c");
			t.setAggregateTags(Arrays.asList("a", "b"));
			addTags(t, "tag", "dom" + i);
			for (int j=0; j < 10; j++) {
				putPoint(t, 2000 + i * 100 + j, 2000 + i * 100 + j);
			}
		}

		TsdbResult[] results = merger.merge(first, second);
		Assert.assertEquals(results.length, first.length);

	}

	@Test
	public void mergeTwoSingleTagResults()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c");

		obj1.setAggregateTags(Arrays.asList("a", "b"));
		obj2.setAggregateTags(Arrays.asList("a", "b"));

		addTags(obj1, "key", "value");
		addTags(obj2, "key", "value");

		//points from 1000 to 1015 in obj1
		for (int i = 1; i <= 15; i++) {
			putPoint(obj1, 1000 + i, 1000 + i);
		}

		//points from 1010 to 1010 in obj2
		for (int i = 10; i <= 20; i++) {
			putPoint(obj2, 1000 + i, 1000 + i);
		}

		// merge should remove the redundant 1010 to 1015 points.

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});

		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);

		TsdbResult result = results[0];
		Assert.assertEquals(result.getMetric(), "a.b.c");

		Assert.assertEquals(result.getAggregateTags().size(), 2);
		Assert.assertTrue(result.getAggregateTags().contains("a"));
		Assert.assertTrue(result.getAggregateTags().contains("b"));

		Assert.assertEquals(result.getDps().getMap().size(), 20,
				"\nfirst=" + obj1.getDps().getMap()
						+ "\nsecond=" + obj2.getDps().getMap()
						+ ",\nresult=" + result.getDps().getMap());

		for (int i=1; i<=20; i++) {
			Assert.assertEquals(result.getDps().getMap().get(String.valueOf(1000+i)), 1000 + i);
		}
	}

	@Test(expectedExceptions = MergeException.class)
	public void tryMergeWithIncompatibleAggTags()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c");

		obj1.setAggregateTags(Arrays.asList("a", "b"));
		obj2.setAggregateTags(Arrays.asList("a", "b", "c"));

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		throw new RuntimeException("Should not reach here");
	}

	@Test(expectedExceptions = MergeException.class)
	public void tryMergeWithNullAggTagsInFirst()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c");

		obj1.setAggregateTags(null);
		obj2.setAggregateTags(Arrays.asList("a", "b", "c"));

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		throw new RuntimeException("Should not reach here");
	}

	@Test(expectedExceptions = MergeException.class)
	public void tryMergeWithNullAggTagsInSecond()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c");

		obj1.setAggregateTags(Arrays.asList("a", "b"));
		obj2.setAggregateTags(null);

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		throw new RuntimeException("Should not reach here");
	}

	@Test(expectedExceptions = MergeException.class)
	public void tryMergeWithIncompatibleTags()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c");

		HashMap<String, String> t1 = new HashMap<>();
		t1.put("a1", "b1");
		t1.put("a2", "b2");
		obj1.setTags(new TsdbResult.Tags(t1));

		HashMap<String, String> t2 = new HashMap<>();
		t2.put("a1", "b1");
		obj2.setTags(new TsdbResult.Tags(t2));

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		throw new RuntimeException("Should not reach here");
	}

	@Test(expectedExceptions = MergeException.class)
	public void tryMergeWithNullTagsInSecond()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c");

		HashMap<String, String> t1 = new HashMap<>();
		t1.put("a1", "b1");
		obj1.setTags(new TsdbResult.Tags(t1));

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		results = merger.merge(new TsdbResult[]{obj2}, new TsdbResult[]{obj1});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		throw new RuntimeException("Should not reach here");
	}

	@Test(expectedExceptions = MergeException.class)
	public void tryMergeWithNullTagsInFirst()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c");

		HashMap<String, String> t1 = new HashMap<>();
		t1.put("a1", "b1");
		obj1.setTags(new TsdbResult.Tags(t1));

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj2}, new TsdbResult[]{obj1});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		throw new RuntimeException("Should not reach here");
	}

	@Test(expectedExceptions = MergeException.class)
	public void tryMergeWithDifferentNames()
	{
		TsdbResult obj1 = get("a.b.c");
		TsdbResult obj2 = get("a.b.c.d");

		TsdbResult[] results = merger.merge(new TsdbResult[]{obj1}, new TsdbResult[]{obj2});
		Assert.assertNotNull(results);
		Assert.assertTrue(results.length == 1);
		Assert.assertEquals(results[0].getMetric(), "a.b.c");

		throw new RuntimeException("Should not reach here");
	}

	static TsdbResult get(String metric)
	{
		TsdbResult obj = new TsdbResult();
		obj.setMetric(metric);
		obj.setAggregateTags(Arrays.asList("host", "domain"));
		return obj;
	}

	static void addFixedPoints(TsdbResult obj)
	{
		Map<String, Object> points = new HashMap<>();
		points.put("1234500000", 11);
		points.put("1234501000", 12);
		points.put("1234502000", 13);
		points.put("1234503000", 14);
		obj.setDps(new TsdbResult.Points(points));
	}

	static void addTags(TsdbResult obj1, String key, String value)
	{
		if (obj1.getTags() == null || obj1.getTags().getTags() == null) {
			obj1.setTags(new TsdbResult.Tags(new HashMap<String, String>()));
		}

		obj1.getTags().getTags().put(key, value);
	}


	static TsdbResult putPoint(TsdbResult result, long timestamp, Object value)
	{
		if (result.getDps() == null || result.getDps().getMap() == null) {
			result.setDps(new TsdbResult.Points(new HashMap<String, Object>()));
		}
		result.getDps().getMap().put(String.valueOf(timestamp), value);
		return result;
	}

}
