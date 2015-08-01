package com.turn.splicer;

import com.turn.splicer.tsdbutils.TsQuery;
import com.turn.splicer.tsdbutils.TsQuerySerializer;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplicerServlet extends HttpServlet {

	private static final Logger LOG = LoggerFactory.getLogger(SplicerServlet.class);

	private static final int NUM_THREADS_PER_POOL = 10;

	private static AtomicInteger POOL_NUMBER = new AtomicInteger(0);

	private static ThreadFactoryBuilder THREAD_FACTORY_BUILDER = new ThreadFactoryBuilder()
			.setDaemon(false)
			.setPriority(Thread.NORM_PRIORITY);

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		try {
			doGetWork(request, response);
		} catch (IOException e) {
			LOG.error("IOException which processing GET request", e);
		} catch (Exception e) {
			LOG.error("Exception which processing GET request", e);
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		try {
			doPostWork(request, response);
		} catch (IOException e) {
			LOG.error("IOException which processing POST request", e);
			e.printStackTrace(response.getWriter());
		} catch (Exception e) {
			LOG.error("Exception which processing POST request", e);
			e.printStackTrace(response.getWriter());
		}
	}

	private void doGetWork(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{

	}

	private void doPostWork(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		BufferedReader reader = request.getReader();
		String line;

		StringBuilder builder = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			builder.append(line);
		}

		String jsonPostRequest = builder.toString();

		TsQuery tsQuery = TsQuerySerializer.deserializeFromJson(jsonPostRequest);
		tsQuery.validateAndSetQuery();

		LOG.info("Serving query={}", tsQuery);

		LOG.info("Original TsQuery Start time={}, End time={}",
				Const.tsFormat(tsQuery.startTime()),
				Const.tsFormat(tsQuery.endTime()));

		long duration = tsQuery.endTime() - tsQuery.startTime();
		if (duration > TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS)) {
			Splicer splicer = new Splicer(tsQuery);
			List<TsQuery> slices = splicer.sliceQuery();
			parallelize(slices);
		} else {
			// only one query. run it in the servlet thread
			HttpWorker worker = new HttpWorker(tsQuery);
			try {
				String res = worker.call();
				LOG.info("Result for singleton query={}", res);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void parallelize(List<TsQuery> slices)
	{
		String poolName = String.format("splice-pool-%d", POOL_NUMBER.incrementAndGet());

		ThreadFactory factory = THREAD_FACTORY_BUILDER
				.setNameFormat(poolName + "-thread-%d")
				.build();

		ExecutorService svc = Executors.newFixedThreadPool(NUM_THREADS_PER_POOL, factory);

		try {
			List<Future<String>> results = new ArrayList<>();
			for (TsQuery q : slices) {
				results.add(svc.submit(new HttpWorker(q)));
			}

			for (Future<String> s: results) {
				String json = s.get();
				LOG.info("Got result={}", json);
			}
		} catch (Exception e) {
			LOG.error("Could not execute HTTP Queries", e);
			throw new RuntimeException(e);
		} finally {
			svc.shutdown();
			LOG.info("Shutdown thread pool");
		}
	}

}
