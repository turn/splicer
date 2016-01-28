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
