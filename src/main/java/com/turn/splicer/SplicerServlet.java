package com.turn.splicer;

import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.hbase.RegionUtil;
import com.turn.splicer.merge.ResultsMerger;
import com.turn.splicer.merge.TsdbResult;
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

	private static RegionUtil REGION_UTIL = new RegionUtil();

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
		LOG.info("GET (from remoteIp=" + request.getRemoteAddr() + ") is not yet supported");
		response.getWriter().write("GET (from " + request.getRemoteAddr() + ") is not yet supported\n");
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

		try (RegionChecker checker = REGION_UTIL.getRegionChecker()) {
			long duration = tsQuery.endTime() - tsQuery.startTime();
			if (duration > TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS)) {
				Splicer splicer = new Splicer(tsQuery);
				List<TsQuery> slices = splicer.sliceQuery();
				String t = parallelize(slices, checker);
				response.getWriter().write(t);
				response.getWriter().flush();
			} else {
				// only one query. run it in the servlet thread
				HttpWorker worker = new HttpWorker(tsQuery, checker);
				try {
					String res = worker.call();
					LOG.info("Result for singleton query={}", res);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	public String parallelize(List<TsQuery> slices, RegionChecker checker)
	{
		String poolName = String.format("splice-pool-%d", POOL_NUMBER.incrementAndGet());

		ThreadFactory factory = THREAD_FACTORY_BUILDER
				.setNameFormat(poolName + "-thread-%d")
				.build();

		ExecutorService svc = Executors.newFixedThreadPool(NUM_THREADS_PER_POOL, factory);
		ResultsMerger merger = new ResultsMerger();
		try {
			List<Future<String>> results = new ArrayList<>();
			for (TsQuery q : slices) {
				results.add(svc.submit(new HttpWorker(q, checker)));
			}

			TsdbResult[] result = null;
			for (Future<String> s: results) {
				String json = s.get();
				LOG.info("Got result={}", json);

				if (result == null) {
					TsdbResult[] tmp = TsdbResult.fromArray(json);
					// set result to tmp iff there are some values
					result = (tmp.length > 0 ? tmp : null);
				} else {
					// we might receive no results for a particular time slot
					TsdbResult[] tmp = TsdbResult.fromArray(json);
					if (tmp.length > 0) {
						result = merger.merge(result, tmp);
					}
				}
			}

			if (result != null) {
				return TsdbResult.toJson(result);
			} else {
				return "[]";
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
