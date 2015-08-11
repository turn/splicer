package com.turn.splicer;

import com.turn.splicer.cache.JedisClient;
import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.tsdbutils.JSON;
import com.turn.splicer.tsdbutils.TsQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpWorker implements Callable<String> {

	private static final Logger LOG = LoggerFactory.getLogger(HttpWorker.class);

	public static final Map<String, LinkedBlockingQueue<String>> TSDMap = new HashMap<>();

	public static Random random = new Random();

	private final TsQuery query;
	private final RegionChecker checker;

	public HttpWorker(TsQuery query, RegionChecker checker) {
		this.query = query;
		this.checker = checker;
	}

	@Override
	public String call() throws Exception
	{
		LOG.info("Start time={}, End time={}", Const.tsFormat(query.startTime()),
				Const.tsFormat(query.endTime()));

		JedisClient jc = new JedisClient();
		String cacheResult = jc.get(this.query.toString());
		if (cacheResult != null) {
			return cacheResult;
		}


		String metricName = query.getQueries().get(0).getMetric();
		String hostname = checker.getBestRegionHost(metricName,
				query.startTime() / 1000, query.endTime() / 1000);
		LOG.info("Found region server hostname={} for metric={}", hostname, metricName);

		LinkedBlockingQueue<String> TSDs;
		if (hostname == null) {
			LOG.error("Could not find region server for metric={}", metricName);
			return "{'error': 'Could not find region server for metric=" + metricName + "'}";
		}

		TSDs = TSDMap.get(hostname);
		if (TSDs == null) {
			LOG.error("We are not running TSDs on regionserver={}. Returning", hostname);
			return "{'error': 'We are not running TSDs on regionserver=" + hostname + "'}";
		}

		String server = TSDs.take();
		String uri = "http://" + server + "/api/query";

		CloseableHttpClient postman = HttpClientBuilder.create().build();
		try {

			HttpPost postRequest = new HttpPost(uri);

			StringEntity input = new StringEntity(JSON.serializeToString(query));
			input.setContentType("application/json");
			postRequest.setEntity(input);

			HttpResponse response = postman.execute(postRequest);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
						+ response.getStatusLine().getStatusCode());
			}

			List<String> dl = IOUtils.readLines(response.getEntity().getContent());
			String result = StringUtils.join(dl, "");
			jc.put(this.query.toString(), result);
			return result;
		} finally {
			IOUtils.closeQuietly(postman);

			TSDs.put(server);
			LOG.info("Put back {} into the queue", server);
		}
	}
}
