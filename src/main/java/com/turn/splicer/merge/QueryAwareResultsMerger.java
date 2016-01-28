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

package com.turn.splicer.merge;

import com.turn.splicer.tsdbutils.TSSubQuery;
import com.turn.splicer.tsdbutils.TsQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryAwareResultsMerger {

	private static final Logger LOG = LoggerFactory.getLogger(QueryAwareResultsMerger.class);

	static final String NO_TAGS = "<no_tags>";

	private final TsQuery query;
	private final TSSubQuery subQuery;

	public QueryAwareResultsMerger(TsQuery query)
	{
		Preconditions.checkNotNull(query, "query is null");
		Preconditions.checkNotNull(query.getQueries(), "null subqueries " + query);
		if (query.getQueries().size() > 1) {
			throw new IllegalArgumentException("too many subqueries. query=" + query);
		}

		this.query = query;
		this.subQuery = query.getQueries().get(0);
	}

	public TsdbResult[] merge(List<TsdbResult[]> slices)
	{
		if (slices == null || slices.size() == 0) {
			return new TsdbResult[]{};
		}

		// if only slice, return it
		if (slices.size() == 1) return slices.get(0);

		// if two slices, merge them
		TsdbResult[] partialMerge = merge(slices.get(0), slices.get(1));

		// for any remaining slices merge with partial result
		for (int i=2; i < slices.size(); i++) {
			partialMerge = merge(partialMerge, slices.get(i));
		}

		// return result
		return partialMerge;
	}

	/**
	 * Merge two results. The result is a new TsdbResult[] object.
	 *
	 * This method searches first creates pairs of TsdbResult objects
	 *
	 * @param left input result for a single slice
	 * @param right another result for a different slice
	 * @return a new object which is the result of merging two partial results. both
	 *         left and right are not mutated.
	 */
	protected TsdbResult[] merge(TsdbResult[] left, TsdbResult[] right)
	{
		if (left == null) left = new TsdbResult[]{};
		if (right == null) right = new TsdbResult[]{};

		Map<String, TsdbResult> leftIndex = index(left);
		List<TsdbResult> mergeResults = new ArrayList<>();

		for (TsdbResult leftItem: right) {
			String ts = createTagString(leftItem);
			TsdbResult rightItem = leftIndex.remove(ts);
			if (rightItem == null) {
				LOG.info("Did not find counterpart for ts={}", ts);
				//continue;
			}
			TsdbResult merge = merge(leftItem, rightItem);
			mergeResults.add(merge);
		}

		// add any left over tags
		if (leftIndex.size() > 0) {
			mergeResults.addAll(leftIndex.values());
		}

		TsdbResult[] r = new TsdbResult[mergeResults.size()];
		mergeResults.toArray(r);
		return r;
	}

	/**
	 *
	 * Merge two results into one. It assumes that the inputs are have common tags.
	 *
	 * @param left a partial result. cannot be null. dps can be null
	 * @param right a partial result. cannot be null. dps can be null
	 * @return if both left and right is null, returns null, if either one is null, returns a
	 *         copy of the non-null result. else, returns a new TsdbResult whose points are a
	 *         union of the points in each input. the other fields are arbitrarily selected
	 *         from one of left or right.
	 */
	protected TsdbResult merge(TsdbResult left, TsdbResult right)
	{

		if (left == null && right == null) return new TsdbResult();
		if (left == null) return right;
		if (right == null) return left;

		// initialize with same metadata, and empty point map
		TsdbResult m = TsdbResult.copyMeta(left);
		m.setDps(new TsdbResult.Points(new TreeMap<String, Object>()));

		// load left points
		if (left.getDps() != null && left.getDps().getMap() != null) {
			for (Map.Entry<String, Object> e : left.getDps().getMap().entrySet()) {
				m.getDps().getMap().put(e.getKey(), e.getValue());
			}
		}

		// load right points
		if (right.getDps() != null && right.getDps().getMap() != null) {
			for (Map.Entry<String, Object> e : right.getDps().getMap().entrySet()) {
				m.getDps().getMap().put(e.getKey(), e.getValue());
			}
		}

		// return
		return m;
	}

	/**
	 * Make an index (string -> tsdbresult) given a TsdbResults[] array, such that each
	 * string key is unique string (created by {@link #createTagString(TsdbResult)} for each
	 * item in the input array.
	 *
	 * @param results input array
	 * @return map of tag string with its respective tsdb result object
	 */
	protected Map<String, TsdbResult> index(TsdbResult[] results)
	{
		Map<String, TsdbResult> index = new HashMap<>();
		for (TsdbResult result: results) {
			index.put(createTagString(result), result);
		}
		return index;
	}

	@VisibleForTesting
	protected String createTagString(TsdbResult result)
	{
		if (result.getTags() == null
				|| result.getTags().getTags() == null
				|| result.getTags().getTags().size() == 0) {
			return NO_TAGS;
		}

		final Set<String> queryTags;
		if (subQuery.getTags() == null || subQuery.getTags().size() == 0) {
			queryTags = new HashSet<>();
		} else {
			queryTags = subQuery.getTags().keySet();
		}

		/**
		 * we don't expect any tags in the query result. return {@link #NO_TAGS}
		 */
		if (queryTags.size() == 0) {
			return NO_TAGS;
		}

		/**
		 * if we are expecting tags in our result, then creates string based on them only
		 */
		Map<String, String> tagMap = result.getTags().getTags();
		String[] tagKeys = tagMap.keySet().toArray(new String[tagMap.size()]);
		Arrays.sort(tagKeys);

		String tagString = "";
		for (String tk: tagKeys) {
			if (queryTags.contains(tk)) {
				if (tagString.length() > 0) tagString += ",";
				tagString += tk + "=" + tagMap.get(tk);
			}
		}

		return tagString;
	}
}
