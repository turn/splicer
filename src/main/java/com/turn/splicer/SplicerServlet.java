package com.turn.splicer;

import com.google.common.collect.Lists;
import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.hbase.RegionUtil;
import com.turn.splicer.merge.ResultsMerger;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.*;
import com.turn.splicer.tsdbutils.expression.Expression;
import com.turn.splicer.tsdbutils.expression.ExpressionTree;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.turn.splicer.tsdbutils.expression.ExpressionTreeWorker;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplicerServlet extends HttpServlet {

	private static final Logger LOG = LoggerFactory.getLogger(SplicerServlet.class);

	public static RegionUtil REGION_UTIL = new RegionUtil();

	private static ExecutorService pool = Executors.newCachedThreadPool();

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
		} catch (Exception e) {
			LOG.error("Exception which processing POST request", e);

			String stackTrace = "";
			if (e.getStackTrace() != null && e.getStackTrace().length > 2) {
				stackTrace += e.getStackTrace()[0] + ", " + e.getStackTrace()[1];
			} else {
				stackTrace = "<empty>";
			}

			response.getWriter().write("{\"error\": \""
					+ e.getMessage() + ", stacktrace=" + stackTrace + "\"}\n");
		}
	}

	/**
	 * Parses a TsQuery out of request, divides into subqueries, and writes result
	 * from openTsdb
	 *
	 * Format for GET request:
	 * start - required start time of query
	 * end - optional end time of query, if not provided will use current time as end
	 * x - expression (functions + metrics)
	 * m - metrics only - no functions, [aggregator]:[optional_downsampling]:metric{optional tags}
	 * either x or m must be provided, otherwise nothing to query!
	 * ms - optional for millisecond resolution
	 * padding - optional pad front of value's with 0's
	 *
	 *example:
	 * /api/query?start=1436910725795&x=abs(sum:1m-avg:tcollector.collector.lines_received)"
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
		}

		//not supporting metric queries from GET yet...
		//TODO: fix this or decide if we need to support "m" type queries
		if(request.getParameter("m") != null) {
			final List<String> legacy_queries = Arrays.asList(request.getParameterValues("m"));
			for(String q: legacy_queries) {
				SplicerUtils.parseMTypeSubQuery(q, dataQuery);
			}
		}

		LOG.info("Serving query={}", dataQuery);

		LOG.info("Original TsQuery Start time={}, End time={}",
				Const.tsFormat(dataQuery.startTime()),
				Const.tsFormat(dataQuery.endTime()));

    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");

		try (RegionChecker checker = REGION_UTIL.getRegionChecker()) {
			List<TsdbResult[]> exprResults = Lists.newArrayList();
			if(expressionTrees == null || expressionTrees.size() == 0) {
				System.out.println("expression trees == null...figure this out later");
				response.getWriter().write("No expression or error parsing expression");
			}
			else {
				try {
					List<Future<TsdbResult[]>> futureList = new ArrayList<Future<TsdbResult[]>>(expressionTrees.size());

					TsQuery prev = null;

					for (ExpressionTree expressionTree : expressionTrees) {
						futureList.add(pool.submit(new ExpressionTreeWorker(expressionTree)));
					}

					for (Future<TsdbResult[]> future : futureList) {
						exprResults.add(future.get());
					}
					response.getWriter().write(TsdbResult.toJson(SplicerUtils.flatten(
							exprResults)));
				} catch (Exception e) {
					LOG.error("Could not evaluate expression tree", e);
					e.printStackTrace();
				}
			}
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

    response.setCharacterEncoding("UTF-8");
    response.setContentType("application/json");

		try (RegionChecker checker = REGION_UTIL.getRegionChecker()) {

			List<TSSubQuery> subQueries = new ArrayList<>(tsQuery.getQueries());
			SplicerQueryRunner queryRunner = new SplicerQueryRunner();

			if (subQueries.size() == 1) {
				TsdbResult[] results = queryRunner.sliceAndRunQuery(tsQuery, checker);
				if (results == null || results.length == 0) {
					response.getWriter().write("[]");
				} else {
					response.getWriter().write(TsdbResult.toJson(results));
				}
				response.getWriter().flush();
			} else {
				List<TsdbResult[]> resultsFromAllSubQueries = new ArrayList<>();
				for (TSSubQuery subQuery: subQueries) {
					TsQuery tsQueryCopy = TsQuery.validCopyOf(tsQuery);
					tsQueryCopy.addSubQuery(subQuery);
					TsdbResult[] results = queryRunner.sliceAndRunQuery(tsQueryCopy, checker);
					resultsFromAllSubQueries.add(results);
				}
				response.getWriter().write(TsdbResult.toJson(SplicerUtils.flatten(
						resultsFromAllSubQueries)));
			}
		}
	}
}
