package com.turn.splicer.tsdbutils.expression;

import com.turn.splicer.hbase.RegionChecker;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.SplicerQueryRunner;
import com.turn.splicer.tsdbutils.TsQuery;

import java.util.concurrent.Callable;

/**
 * Created by bpeltz on 10/22/15.
 */
public class QueryRunnerWorker implements Callable<TsdbResult[]> {

	private SplicerQueryRunner queryRunner;

	private TsQuery query;

	private RegionChecker checker;

	public QueryRunnerWorker(SplicerQueryRunner queryRunner, TsQuery query, RegionChecker checker) {
		this.queryRunner = queryRunner;
		this.query = query;
		this.checker = checker;
	}
	@Override
	public TsdbResult[] call() throws Exception {
		return queryRunner.sliceAndRunQuery(query, checker);
	}
}
