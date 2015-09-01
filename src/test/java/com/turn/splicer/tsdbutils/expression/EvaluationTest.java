package com.turn.splicer.tsdbutils.expression;

import com.google.common.collect.Lists;
import com.turn.splicer.merge.TsdbResult;
import org.testng.annotations.Test;

import java.util.List;

@Test
public class EvaluationTest {

    @Test
    public void parseNestedExpr() {
        String expr = "id(sum:proc.meminfo.buffers,, id(sum:proc.meminfo.buffers))";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries, null);
        System.out.println(metricQueries);

        tree.evaluate(Lists.<TsdbResult[]>newArrayList(null, null));
    }

}
