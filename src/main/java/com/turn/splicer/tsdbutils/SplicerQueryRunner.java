package com.turn.splicer.tsdbutils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.turn.splicer.Const;
import com.turn.splicer.HttpWorker;
import com.turn.splicer.Splicer;
import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.merge.QueryAwareResultsMerger;
import com.turn.splicer.merge.ResultsMerger;
import com.turn.splicer.merge.TsdbResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * "Slices" a single TsQuery into multiple TsQuery objects that span the TsQuery
 * time range, runs those mulitple TsQuerys in parallel and splices
 * the results together into a TsdbResult[]
 */
public class SplicerQueryRunner {

	private static final Logger LOG = LoggerFactory.getLogger(SplicerQueryRunner.class);

	private static AtomicInteger POOL_NUMBER = new AtomicInteger(0);

	private static final int NUM_THREADS_PER_POOL = 10;

	private static ThreadFactoryBuilder THREAD_FACTORY_BUILDER = new ThreadFactoryBuilder()
			.setDaemon(false)
			.setPriority(Thread.NORM_PRIORITY);

	public TsdbResult[] sliceAndRunQuery(TsQuery tsQuery, RegionChecker checker)
			throws IOException
	{
		long duration = tsQuery.endTime() - tsQuery.startTime();
		if (duration > TimeUnit.MILLISECONDS.convert(2, TimeUnit.HOURS)) {
			Splicer splicer = new Splicer(tsQuery);
			List<TsQuery> slices = splicer.sliceQuery();
			return runQuerySlices(tsQuery, slices, checker);
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

	private TsdbResult[] runQuerySlices(TsQuery query, List<TsQuery> slices, RegionChecker checker)
	{
		String poolName = String.format("splice-pool-%d", POOL_NUMBER.incrementAndGet());

		ThreadFactory factory = THREAD_FACTORY_BUILDER
				.setNameFormat(poolName + "-thread-%d")
				.build();

		ExecutorService svc = Executors.newFixedThreadPool(NUM_THREADS_PER_POOL, factory);
		ResultsMerger merger = new ResultsMerger();
		QueryAwareResultsMerger qamerger = new QueryAwareResultsMerger(query);
		try {
			List<Future<String>> results = new ArrayList<>();
			for (TsQuery q : slices) {
				results.add(svc.submit(new HttpWorker(q, checker)));
			}

			List<TsdbResult[]> tmpResults = new ArrayList<>();
			for (Future<String> s: results) {
				String json = s.get();
				LOG.debug("Got result={}", json);

				TsdbResult[] r = TsdbResult.fromArray(json);
				tmpResults.add(r);
			}

			TsdbResult[] qaResult = qamerger.merge(tmpResults);
			// respond with qaResult
			if (qaResult != null) {
				return qaResult;
			} else {
				return new TsdbResult[]{};
			}

		} catch (Exception e) {
			LOG.error("Could not execute HTTP Queries", e);
			throw new RuntimeException(e);
		} finally {
			svc.shutdown();
			LOG.info("Shutdown thread pool for query=" + stringify(query));
		}
	}

	private String stringify(TsQuery query)
	{
		String subs = "";
		for (TSSubQuery sub: query.getQueries()) {
			if (subs.length() > 0) subs += ",";
			subs += "m=[" + sub.getMetric() + sub.getTags()
					+ ", downsample=" + sub.getDownsample()
					+ ", rate=" + sub.getRate()
					+ ", tags=" + sub.getTags()
					+ "]";
		}
		return "{" + query.startTime() + " to " + query.endTime() + ", " + subs + "}";
	}

}
