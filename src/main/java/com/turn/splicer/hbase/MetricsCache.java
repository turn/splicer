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

package com.turn.splicer.hbase;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCache {

	private static final Logger LOG = LoggerFactory.getLogger(MetricsCache.class);

	protected RegionUtil regionUtil = new RegionUtil();

	protected LoadingCache<String, byte[]> cache = null;

	private static MetricsCache CACHE = new MetricsCache();

	private MetricsCache() {
		cache = CacheBuilder.newBuilder()
				.maximumSize(1_000_000)
				.build(new KeyFetcher(regionUtil));
	}

	public static MetricsCache get() {
		return CACHE;
	}

	static class KeyFetcher extends CacheLoader<String, byte[]> {

		private final RegionUtil regionUtil;

		public KeyFetcher(RegionUtil regionUtil) {
			this.regionUtil = regionUtil;
		}

		@Override
		public byte[] load(String metric) throws Exception {
			Configuration config = regionUtil.getConfig();
			try (HTable table = new HTable(config, "tsdb-uid")) {
				Get get = new Get(toBytes(metric));
				get.addColumn(toBytes("id"), toBytes("metrics"));
				Result result = table.get(get);
				LOG.info("Looking up key for metric={}. Found result={}",
						metric, Arrays.toString(result.value()));
				return result.value();
			}
		}
	}

	static byte[] toBytes(String str) {
		return str.getBytes(Charset.forName("ISO-8859-1"));
	}

	public byte[] getMetricKey(String metric) {
		try {
			return cache.get(metric);
		} catch (ExecutionException e) {
			LOG.error("Error loading value to cache", e);
			throw new MetricLookupException("Error loading value to cache", e);
		}
	}
}
