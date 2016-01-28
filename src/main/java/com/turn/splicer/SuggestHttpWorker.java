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

package com.turn.splicer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Takes a api/suggest query string and runs it against a random TSD node,
 * returns the result of the query
 *
 * TODO: needs to be refactored with HttpWorker, there is a lot of duplicate logic
 * TODO: error reporting differs from regular TSDB - no error message for invalid type
 */
public class SuggestHttpWorker implements Callable<String> {
	private static final Logger LOG = LoggerFactory.getLogger(SuggestHttpWorker.class);

	private final String suggestQuery;

	private final String[] hosts;

	private static final Random RANDOM_GENERATOR = new Random();

	public SuggestHttpWorker(String queryString) {
		this.suggestQuery = queryString;

		Set<String> hosts = HttpWorker.TSDMap.keySet();
		if (hosts.size() == 0) {
			throw new NullPointerException("No Query Hosts. TSDMap.size = 0");
		}

		this.hosts = new String[hosts.size()];
		int i=0;
		for (String h: hosts) {
			this.hosts[i] = h;
			i++;
		}
	}

	@Override
	public String call() throws Exception {
		LinkedBlockingQueue<String> TSDs;

		//TODO: have it implement its own RegionChecker to get hbase locality looking for metric names
		//lets have it just pick a random host
		String hostname = getRandomHost();
		TSDs = HttpWorker.TSDMap.get(hostname);

		if (TSDs == null) {
			LOG.error("We are not running TSDs on regionserver={}. Choosing a random host failed", hostname);
			return "{'error': 'Choice of hostname=" + hostname + " failed.'}";
		}

		String server = TSDs.take();
		String uri = "http://" + server + "/api/suggest?" + suggestQuery;

		CloseableHttpClient postman = HttpClientBuilder.create().build();
		try {
			HttpGet getRequest = new HttpGet(uri);

			LOG.info("Sending query=" + uri + " to TSD running on host=" + hostname);

			HttpResponse response = postman.execute(getRequest);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatusLine().getStatusCode());
			}

			List<String> dl = IOUtils.readLines(response.getEntity().getContent());
			String result = StringUtils.join(dl, "");
			LOG.info("Result={}", result);

			return result;
		} finally {
			IOUtils.closeQuietly(postman);

			TSDs.put(server);
			LOG.info("Returned {} into the available queue", server);
		}
	}

	/**
	 * Chooses a random number between 0 - (host.length-1) inclusive
	 * returns that index in the hosts list
	 * @return
	 */
	private String getRandomHost() {
		return hosts[RANDOM_GENERATOR.nextInt(hosts.length)];
	}
}
