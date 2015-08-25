package com.turn.splicer;

import com.google.common.collect.Lists;
import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.hbase.RegionUtil;
import com.turn.splicer.merge.ResultsMerger;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.turn.splicer.tsdbutils.expression.ExpressionTree;
import com.turn.splicer.tsdbutils.expression.parser.ParseException;
import com.turn.splicer.tsdbutils.expression.parser.SyntaxChecker;
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

		final TsQuery data_query = new TsQuery();

		data_query.setStart(request.getParameter("start"));
		data_query.setEnd(request.getParameter("end"));

		data_query.setPadding(Boolean.valueOf(request.getParameter("padding")));

		if(request.getParameter("ms") != null) {
			data_query.setMsResolution(true);
		}

		List<ExpressionTree> expressionTrees = null;

		final String[] expressions = request.getParameterValues("x");
		if(expressions != null) {
			expressionTrees = new ArrayList<ExpressionTree>();
			List<String> metricQueries = new ArrayList<String>();
			this.syntaxCheck(expressions, data_query, metricQueries, expressionTrees);

			for(String mq: metricQueries) {
					this.parseMTypeSubQuery(mq, data_query);
			}
		}

		if(request.getParameter("m") != null) {
			final List<String> legacy_queries = Arrays.asList(request.getParameterValues("m"));
			for(String q: legacy_queries) {
				this.parseMTypeSubQuery(q, data_query);
			}
		}

		data_query.validateAndSetQuery();

		LOG.info("Serving query={}", data_query);

		LOG.info("Original TsQuery Start time={}, End time={}",
				Const.tsFormat(data_query.startTime()),
				Const.tsFormat(data_query.endTime()));

		try (RegionChecker checker = REGION_UTIL.getRegionChecker()) {

			List<TSSubQuery> subQueries = new ArrayList<>(data_query.getQueries());
			List<TsdbResult[]> resultsFromAllSubQueries = new ArrayList<>();

			if(subQueries.size() == 0) {
				throw new BadRequestException("No subqueries");
			}
			else if (subQueries.size() == 1) {
				TsdbResult[] results = parallelizePerSubQuery(data_query, checker);
				resultsFromAllSubQueries.add(results);
			} else {
				for (TSSubQuery subQuery: subQueries) {
					TsQuery tsQueryCopy = TsQuery.validCopyOf(data_query);
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
				response.getWriter().write(TsdbResult.toJson(flatten(exprResults)));
			} else {
				response.getWriter().write(TsdbResult.toJson(flatten(resultsFromAllSubQueries)));
			}
			response.getWriter().flush();
		}
	}

	/**
	 * Parses expression out of exprs, adds them to TsQuery
	 * @param exprs
	 * @param tsQuery
	 * @param metricQueries
	 * @param expressionTrees
	 */
	private void syntaxCheck(String[] exprs, TsQuery tsQuery, List<String> metricQueries, List<ExpressionTree> expressionTrees) {
		for (String expr : exprs) {
			SyntaxChecker checker = new SyntaxChecker(new StringReader(expr));
			checker.setMetricQueries(metricQueries);
			checker.setTsQuery(tsQuery);
			try {
				ExpressionTree tree = checker.EXPRESSION();
				expressionTrees.add(tree);
			} catch (ParseException e) {
				throw new RuntimeException("Could not parse " + expr, e);
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
				response.getWriter().write(TsdbResult.toJson(flatten(resultsFromAllSubQueries)));
				response.getWriter().flush();
			}
		}
	}

	/**
	 * Parse out rate, downsample, aggregator from metric query and adds new query
	 * into TsQuery
	 * @param query_string
	 * @param data_query
	 */
	private void parseMTypeSubQuery(final String query_string,
									TsQuery data_query) {
		if (query_string == null || query_string.isEmpty()) {
			throw new BadRequestException("The query string was empty");
		}

		// m is of the following forms:
		// agg:[interval-agg:][rate:]metric[{tag=value,...}]
		// where the parts in square brackets `[' .. `]' are optional.
		final String[] parts = this.splitString(query_string, ':');
		int i = parts.length;
		if (i < 2 || i > 5) {
			throw new BadRequestException("Invalid parameter m=" + query_string + " ("
					+ (i < 2 ? "not enough" : "too many") + " :-separated parts)");
		}
		final TSSubQuery sub_query = new TSSubQuery();

		// the aggregator is first
		sub_query.setAggregator(parts[0]);

		i--; // Move to the last part (the metric name).
		HashMap<String, String> tags = new HashMap<String, String>();
		sub_query.setMetric(this.parseWithMetric(parts[i], tags));
		sub_query.setTags(tags);

		// parse out the rate and downsampler
		for (int x = 1; x < parts.length - 1; x++) {
			if (parts[x].toLowerCase().startsWith("rate")) {
				sub_query.setRate(true);
				if (parts[x].indexOf("{") >= 0) {
					sub_query.setRateOptions(this.parseRateOptions(true, parts[x]));
				}
			} else if (Character.isDigit(parts[x].charAt(0))) {
				sub_query.setDownsample(parts[x]);
			}
		}

		if (data_query.getQueries() == null) {
			final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
			data_query.setQueries(subs);
		}
		data_query.getQueries().add(sub_query);
	}

	/**
	 * Parses the metric and tags out of the given string.
	 *
	 * @param metric A string of the form "metric" or "metric{tag=value,...}".
	 * @param tags   The map to populate with the tags parsed out of the first
	 *               argument.
	 * @return The name of the metric.
	 * @throws IllegalArgumentException if the metric is malformed.
	 */
	public static String parseWithMetric(final String metric,
										 final HashMap<String, String> tags) {
		final int curly = metric.indexOf('{');
		if (curly < 0) {
			return metric;
		}
		final int len = metric.length();
		if (metric.charAt(len - 1) != '}') {  // "foo{"
			throw new IllegalArgumentException("Missing '}' at the end of: " + metric);
		} else if (curly == len - 2) {  // "foo{}"
			return metric.substring(0, len - 2);
		}
		// substring the tags out of "foo{a=b,...,x=y}" and parse them.
		for (final String tag : splitString(metric.substring(curly + 1, len - 1),
				',')) {
			try {
				parse(tags, tag);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("When parsing tag '" + tag
						+ "': " + e.getMessage());
			}
		}
		// Return the "foo" part of "foo{a=b,...,x=y}"
		return metric.substring(0, curly);
	}

	/**
	 * Parses a tag into a HashMap.
	 *
	 * @param tags The HashMap into which to store the tag.
	 * @param tag  A String of the form "tag=value".
	 * @throws IllegalArgumentException if the tag is malformed.
	 * @throws IllegalArgumentException if the tag was already in tags with a
	 *                                  different value.
	 */
	public static void parse(final HashMap<String, String> tags,
							 final String tag) {
		final String[] kv = splitString(tag, '=');
		if (kv.length != 2 || kv[0].length() <= 0 || kv[1].length() <= 0) {
			throw new IllegalArgumentException("invalid tag: " + tag);
		}
		if (kv[1].equals(tags.get(kv[0]))) {
			return;
		}
		if (tags.get(kv[0]) != null) {
			throw new IllegalArgumentException("duplicate tag: " + tag
					+ ", tags=" + tags);
		}
		tags.put(kv[0], kv[1]);
	}

	/**
	 * Optimized version of {@code String#split} that doesn't use regexps.
	 * This function works in O(5n) where n is the length of the string to
	 * split.
	 *
	 * @param s The string to split.
	 * @param c The separator to use to split the string.
	 * @return A non-null, non-empty array.
	 */
	public static String[] splitString(final String s, final char c) {
		final char[] chars = s.toCharArray();
		int num_substrings = 1;
		for (final char x : chars) {
			if (x == c) {
				num_substrings++;
			}
		}
		final String[] result = new String[num_substrings];
		final int len = chars.length;
		int start = 0;  // starting index in chars of the current substring.
		int pos = 0;    // current index in chars.
		int i = 0;      // number of the current substring.
		for (; pos < len; pos++) {
			if (chars[pos] == c) {
				result[i++] = new String(chars, start, pos - start);
				start = pos + 1;
			}
		}
		result[i] = new String(chars, start, pos - start);
		return result;
	}

	private TsdbResult[] flatten(List<TsdbResult[]> allResults) throws IOException
	{
		int size = 0;
		for (TsdbResult[] r: allResults) {
			size += r.length;
		}

		int i=0;
		TsdbResult[] array = new TsdbResult[size];
		for (TsdbResult[] r: allResults) {
			for (TsdbResult s: r) {
				array[i] = s;
				i++;
			}
		}

		return array;
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

	static final public RateOptions parseRateOptions(final boolean rate,
													 final String spec) {
		if (!rate || spec.length() == 4) {
			return new RateOptions(false, Long.MAX_VALUE,
					RateOptions.DEFAULT_RESET_VALUE);
		}

		if (spec.length() < 6) {
			throw new BadRequestException("Invalid rate options specification: "
					+ spec);
		}

		String[] parts = splitString(spec.substring(5, spec.length() - 1), ',');
		if (parts.length < 1 || parts.length > 3) {
			throw new BadRequestException(
					"Incorrect number of values in rate options specification, must be " +
							"counter[,counter max value,reset value], recieved: "
							+ parts.length + " parts");
		}

		final boolean counter = "counter".equals(parts[0]);
		try {
			final long max = (parts.length >= 2 && parts[1].length() > 0 ? Long
					.parseLong(parts[1]) : Long.MAX_VALUE);
			try {
				final long reset = (parts.length >= 3 && parts[2].length() > 0 ? Long
						.parseLong(parts[2]) : RateOptions.DEFAULT_RESET_VALUE);
				return new RateOptions(counter, max, reset);
			} catch (NumberFormatException e) {
				throw new BadRequestException(
						"Reset value of counter was not a number, received '" + parts[2]
								+ "'");
			}
		} catch (NumberFormatException e) {
			throw new BadRequestException(
					"Max value of counter was not a number, received '" + parts[1] + "'");
		}
	}
}
