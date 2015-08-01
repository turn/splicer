package com.turn.splicer;

import com.turn.splicer.tsdbutils.JSON;
import com.turn.splicer.tsdbutils.TsQuery;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.io.Closeables;
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

	public static final LinkedBlockingQueue<String> TSDs = new LinkedBlockingQueue<>();

	private final TsQuery query;

	public HttpWorker(TsQuery query) {
		this.query = query;
	}

	@Override
	public String call() throws Exception
	{
		LOG.info("Start time={}, End time={}", Const.tsFormat(query.startTime()),
				Const.tsFormat(query.endTime()));

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
			return StringUtils.join(dl, "");
		} finally {
			IOUtils.closeQuietly(postman);

			TSDs.put(server);
			LOG.info("Put back {} into the queue", server);
		}
	}
}
