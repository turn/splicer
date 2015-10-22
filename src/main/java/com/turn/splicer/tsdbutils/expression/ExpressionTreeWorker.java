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
