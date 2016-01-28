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

import com.turn.splicer.merge.TsdbResult;

import java.util.concurrent.Callable;

/**
 * Callable class that evaluates an expressionTree
 * Created by bpeltz on 10/21/15.
 */
public class ExpressionTreeWorker implements Callable<TsdbResult[]> {

	private final ExpressionTree expressionTree;

	public ExpressionTreeWorker(ExpressionTree et) {
		this.expressionTree = et;
	}

	@Override
	public TsdbResult[] call() throws Exception {
		return expressionTree.evaluateAll();
	}
}
