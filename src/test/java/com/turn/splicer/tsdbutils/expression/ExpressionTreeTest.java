package com.turn.splicer.tsdbutils.expression;

import com.google.common.collect.Lists;
import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.TsQuery;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ExpressionTreeTest {

    @BeforeClass
    public static void setup() {
        ExpressionFactory.addFunction("foo", new FooExpression());
    }

    @Test
    public void parseSimple() {
        String expr = "foo(sum:proc.sys.cpu)";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries, null);
        System.out.println(metricQueries);
    }

    @Test
    public void parseMultiParameter() {
        String expr = "foo(sum:proc.sys.cpu,, sum:proc.meminfo.memfree)";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries, null);
        System.out.println(metricQueries);
    }

    @Test
    public void parseNestedExpr() {
        String expr = "foo(sum:proc.sys.cpu,, foo(sum:proc.a.b))";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries, null);
        System.out.println(metricQueries);
    }

    @Test
    public void parseExprWithParam() {
        String expr = "foo(sum:proc.sys.cpu,, 100,, 3.1415)";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries, null);
        System.out.println(metricQueries);
    }

    static class FooExpression implements Expression {

      @Override
      public TsdbResult[] evaluate(TsQuery data_query, List<TsdbResult[]> queryResults, List<String> queryParams) {
        return new TsdbResult[0];
      }

      @Override
      public String writeStringField(List<String> queryParams, String innerExpression) {
        return "foo(" + innerExpression + ")";
      }
    }
}
