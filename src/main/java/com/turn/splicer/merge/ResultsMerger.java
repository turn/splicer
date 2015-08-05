package com.turn.splicer.merge;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class ResultsMerger {

	/**
	 * Merge two chunks of results into one piece
	 * @param first Results for one slice
	 * @param second Results for another slice from same query
	 * @return a third chunk which should have points from first and second chunks
	 */
	public TsdbResult[] merge (TsdbResult[] first, TsdbResult[] second) {
		Preconditions.checkNotNull(first);
		Preconditions.checkNotNull(second);
		Preconditions.checkArgument(first.length > 0);
		Preconditions.checkArgument(second.length == first.length);

		String metricName = first[0].getMetric();
		for (int i = 1; i < first.length; i++) {
			if (!first[i].getMetric().equals(metricName)) {
				throw new MergeException("Metric Names differ within first result. Metric="
						+ metricName + ", first has=" + first[i].getMetric());
			}
		}

		for (TsdbResult s: second) {
			if (!s.getMetric().equals(metricName)) {
				throw new MergeException("Metric Names differ in second result. Metric="
						+ metricName + ", second has=" + s.getMetric());
			}
		}

		Map<Long, List<TsdbResult>> fTable = new HashMap<>();
		for (TsdbResult f: first) {
			long signature = signatureOf(f);
			if (fTable.containsKey(signature)) {
				List<TsdbResult> list = fTable.get(signature);
				list.add(f);
			} else {
				fTable.put(signature, Lists.newArrayList(f));
			}
		}

		List<TsdbResult> finalResults = new ArrayList<>();
		for (TsdbResult s: second) {
			long signature = signatureOf(s);
			List<TsdbResult> fromFirst = fTable.get(signature);
			if (fromFirst == null || fromFirst.isEmpty()) {
				throw new MergeException("Could not find counterpart for " + s);
			}

			boolean foundMatch = false;
			if (fromFirst.size() == 1) {
				finalResults.add(merge(fromFirst.get(0), s));
				foundMatch = true;
			} else {
				for (TsdbResult r: fromFirst) {
					if (identical(r, s)) {
						finalResults.add(merge(r, s));
						foundMatch = true;
					}
				}
			}

			if (!foundMatch) {
				throw new MergeException("Could not find match for s=" + s);
			}
		}

		return finalResults.toArray(new TsdbResult[finalResults.size()]);
	}

	/**
	 * blind merge
	 * @param first first result
	 * @param second second result
	 */
	private TsdbResult merge(TsdbResult first, TsdbResult second) {
		TsdbResult result = new TsdbResult();
		result.setMetric(first.getMetric());
		result.setTags(first.getTags());
		result.setAggregateTags(first.getAggregateTags());
		result.setTsuids(first.getTsuids());

		Map<String, Object> points = new TreeMap<>();
		points.putAll(first.getDps().getMap());
		points.putAll(second.getDps().getMap());

		result.setDps(new TsdbResult.Points(points));
		return result;
	}

	public boolean identical(TsdbResult one, TsdbResult two)
	{
		List<String> aggTagsOne = one.getAggregateTags();
		List<String> aggTagsTwo = two.getAggregateTags();
		if (aggTagsOne != null && aggTagsTwo != null && !aggTagsOne.containsAll(aggTagsTwo)) {
				return false;
		} else if (aggTagsOne == null && aggTagsTwo != null) {
			return false;
		} else if (aggTagsOne != null && aggTagsTwo == null) {
			return false;
		}

		Map<String, String> tagsOne = one.getTags().getTags();
		Map<String, String> tagsTwo = two.getTags().getTags();
		if (tagsOne != null && tagsTwo == null) {
			return false;
		} else if (tagsOne == null && tagsTwo != null) {
			return false;
		}

		if (tagsOne == null) tagsOne = new HashMap<>();
		if (tagsTwo == null) tagsTwo = new HashMap<>();

		if ( !tagsOne.keySet().containsAll(tagsTwo.keySet()) ) return false;
		if ( !tagsOne.values().containsAll(tagsTwo.values()) ) return false;

		List<String> tsuidsOne = one.getTsuids();
		List<String> tsuidsTwo = two.getTsuids();
		if (tsuidsOne != null && tsuidsTwo != null && !tsuidsOne.containsAll(tsuidsTwo)) {
			return false;
		} else if (tsuidsOne == null && tsuidsTwo != null) {
			return false;
		} else if (tsuidsOne != null && tsuidsTwo == null) {
			return false;
		}

		return true;
	}

	public long signatureOf(TsdbResult result) {
		HashFunction hf = Hashing.goodFastHash(64);
		Hasher hasher = hf.newHasher();

		List<String> aggTags = result.getAggregateTags();
		if (aggTags != null) {
			List<String> sortedAggTags = Lists.newArrayList(aggTags);
			Collections.sort(sortedAggTags);
			for (String aggTag: sortedAggTags) {
				hasher.putString(aggTag, Charset.forName("ISO-8859-1"));
			}
		}

		Map<String, String> tags;
		if (result.getTags() != null && (tags = result.getTags().getTags()) != null) {
			List<String> tagTokens = Lists.newArrayList(tags.keySet());
			Collections.sort(tagTokens);
			for (String s: tagTokens) {
				hasher.putString(s, Charset.forName("ISO-8859-1"));
				hasher.putString(tags.get(s), Charset.forName("ISO-8859-1"));
			}
		}

		List<String> tsuids = result.getTsuids();
		if (tsuids != null) {
			List<String> sortedTsUIDs = Lists.newArrayList(tsuids);
			Collections.sort(sortedTsUIDs);
			for (String tsuid: sortedTsUIDs) {
				hasher.putString(tsuid, Charset.forName("ISO-8859-1"));
			}
		}

		return hasher.hash().asLong();
	}

}
