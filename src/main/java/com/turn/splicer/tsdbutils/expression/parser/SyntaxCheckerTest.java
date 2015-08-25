/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package com.turn.splicer.tsdbutils.expression.parser;


import com.turn.splicer.tsdbutils.TsQuery;
import com.turn.splicer.tsdbutils.expression.ExpressionTree;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class SyntaxCheckerTest {

	public static void main(String[] args) {
		try {
			StringReader r = new StringReader("sum(sum:proc.stat.cpu.percpu{cpu=1})");
			SyntaxChecker checker = new SyntaxChecker(r);
			TsQuery query = new TsQuery();
			List<String> metrics = new ArrayList<String>();
			checker.setTsQuery(query);
			checker.setMetricQueries(metrics);
			ExpressionTree tree = checker.EXPRESSION();
			System.out.println("Syntax is okay. ExprTree=" + tree);
			System.out.println("Metrics=" + metrics);
		} catch (Throwable e) {
			System.out.println("Syntax check failed: " + e);
		}
	}
}
