package com.turn.splicer;

import com.google.common.collect.Lists;
import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.hbase.RegionUtil;
import com.turn.splicer.merge.ResultsMerger;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.BadRequestException;
import com.turn.splicer.tsdbutils.SplicerUtils;
import com.turn.splicer.tsdbutils.TSSubQuery;
import com.turn.splicer.tsdbutils.TsQuery;
import com.turn.splicer.tsdbutils.TsQuerySerializer;
import com.turn.splicer.tsdbutils.expression.ExpressionTree;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

	/**
	 * Parses a TsQuery out of request, divides into subqueries, and writes result
	 * from openTsdb
	 * @param request
	 * @param response
	 * @throws IOException
	 */
	private void doGetWork(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		LOG.info(request.getQueryString());

		final TsQuery dataQuery = new TsQuery();

		dataQuery.setStart(request.getParameter("start"));
		dataQuery.setEnd(request.getParameter("end"));

		dataQuery.setPadding(Boolean.valueOf(request.getParameter("padding")));

		if(request.getParameter("ms") != null) {
			dataQuery.setMsResolution(true);
		}

		List<ExpressionTree> expressionTrees = null;

		final String[] expressions = request.getParameterValues("x");
		if(expressions != null) {
			expressionTrees = new ArrayList<ExpressionTree>();
			List<String> metricQueries = new ArrayList<String>();
			SplicerUtils.syntaxCheck(expressions, dataQuery, metricQueries, expressionTrees);

			for(String mq: metricQueries) {
					SplicerUtils.parseMTypeSubQuery(mq, dataQuery);
			}
		}

		if(request.getParameter("m") != null) {
			final List<String> legacy_queries = Arrays.asList(request.getParameterValues("m"));
			for(String q: legacy_queries) {
				SplicerUtils.parseMTypeSubQuery(q, dataQuery);
			}
		}

		dataQuery.validateAndSetQuery();

		LOG.info("Serving query={}", dataQuery);

		LOG.info("Original TsQuery Start time={}, End time={}",
				Const.tsFormat(dataQuery.startTime()),
				Const.tsFormat(dataQuery.endTime()));

		try (RegionChecker checker = REGION_UTIL.getRegionChecker()) {

			List<TSSubQuery> subQueries = new ArrayList<>(dataQuery.getQueries());
			List<TsdbResult[]> resultsFromAllSubQueries = new ArrayList<>();

			if(subQueries.size() == 0) {
				throw new BadRequestException("No subqueries");
			}
			else if (subQueries.size() == 1) {
				TsdbResult[] results = parallelizePerSubQuery(dataQuery, checker);
				resultsFromAllSubQueries.add(results);
			} else {
				for (TSSubQuery subQuery: subQueries) {
					TsQuery tsQueryCopy = TsQuery.validCopyOf(dataQuery);
					tsQueryCopy.getQueries().add(subQuery);
					TsdbResult[] results = parallelizePerSubQuery(tsQueryCopy, checker);
					resultsFromAllSubQueries.add(results);
				}
			}

			List<TsdbResult[]> exprResults = Lists.newArrayList();

			if(expressionTrees != null && expressionTrees.size() > 0) {
				for(ExpressionTree tree : expressionTrees) {
					exprResults.add(tree.evaluate(resultsFromAllSubQueries));
				}
				response.getWriter().write(TsdbResult.toJson(SplicerUtils.flatten(
						exprResults)));
			} else {
				response.getWriter().write(TsdbResult.toJson(SplicerUtils.flatten(
						resultsFromAllSubQueries)));
			}
			response.getWriter().flush();
		}
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

			List<TSSubQuery> subQueries = new ArrayList<>(tsQuery.getQueries());
			if (subQueries.size() == 1) {
				TsdbResult[] results = parallelizePerSubQuery(tsQuery, checker);
				response.getWriter().write(TsdbResult.toJson(results));
				response.getWriter().flush();
			} else {
				List<TsdbResult[]> resultsFromAllSubQueries = new ArrayList<>();
				for (TSSubQuery subQuery: subQueries) {
					TsQuery tsQueryCopy = TsQuery.validCopyOf(tsQuery);
					tsQueryCopy.getQueries().add(subQuery);
					TsdbResult[] results = parallelizePerSubQuery(tsQueryCopy, checker);
					resultsFromAllSubQueries.add(results);
				}
				response.getWriter().write(TsdbResult.toJson(SplicerUtils.flatten(
						resultsFromAllSubQueries)));
				response.getWriter().flush();
			}
		}
	}

	public TsdbResult[] parallelizePerSubQuery(TsQuery tsQuery, RegionChecker checker)
			throws IOException
	{
		long duration = tsQuery.endTime() - tsQuery.startTime();
		if (duration > TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS)) {
			Splicer splicer = new Splicer(tsQuery);
			List<TsQuery> slices = splicer.sliceQuery();
			return parallelize(slices, checker);
		} else {
			// only one query. run it in the servlet thread
			HttpWorker worker = new HttpWorker(tsQuery, checker);
			try {
				String json = worker.call();
				return TsdbResult.fromArray(json);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public TsdbResult[] parallelize(List<TsQuery> slices, RegionChecker checker)
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
				return result;
			} else {
				return new TsdbResult[]{};
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
