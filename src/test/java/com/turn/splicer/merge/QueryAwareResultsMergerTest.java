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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QueryAwareResultsMergerTest {

	private static final Logger LOG = LoggerFactory.getLogger(QueryAwareResultsMergerTest.class);

	@Test
	public void testCreateTagString()
	{
		TsdbResult result = new TsdbResult();
		TsQuery query = new TsQuery();
		query.addSubQuery(new TSSubQuery());
		QueryAwareResultsMerger merger = new QueryAwareResultsMerger(query);

		Assert.assertEquals(QueryAwareResultsMerger.NO_TAGS, merger.createTagString(result));

		result = new TsdbResult();
		Map<String, String> m = new HashMap<>();
		m.put("x", "b");
		result.setTags(new TsdbResult.Tags(m));

		// we should no tags inspite of there being tags in the
		Assert.assertEquals(QueryAwareResultsMerger.NO_TAGS, merger.createTagString(result));

		// expect some tags in the result
		HashMap<String, String> expectedTags = new HashMap<>();
		expectedTags.put("x", "b");
		expectedTags.put("y", "b");
		expectedTags.put("z", "b");
		query.getQueries().get(0).setTags(expectedTags);
		result = new TsdbResult();
		m = new HashMap<>();
		m.put("x", "b");
		m.put("y", "b");
		m.put("z", "b");
		result.setTags(new TsdbResult.Tags(m));

		// result should have all three tags
		Assert.assertEquals("x=b,y=b,z=b", merger.createTagString(result));

		// expect fewer tags that what is sent back
		expectedTags = new HashMap<>();
		expectedTags.put("x", "b");
		expectedTags.put("z", "b");
		query.getQueries().get(0).setTags(expectedTags);
		result = new TsdbResult();
		m = new HashMap<>();
		m.put("x", "b");
		m.put("y", "b");
		m.put("z", "b");
		result.setTags(new TsdbResult.Tags(m));

		// result should have all three tags
		Assert.assertEquals("x=b,z=b", merger.createTagString(result));
	}

}
