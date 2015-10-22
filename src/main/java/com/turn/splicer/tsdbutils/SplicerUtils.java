package com.turn.splicer.tsdbutils;

import com.turn.splicer.merge.TsdbResult;
import com.turn.splicer.tsdbutils.expression.ExpressionTree;
import com.turn.splicer.tsdbutils.expression.parser.ParseException;
import com.turn.splicer.tsdbutils.expression.parser.SyntaxChecker;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Contains static methods used by SplicerServlet for parsing information out of
 * query
 */
public final class SplicerUtils {
    public static final  RateOptions parseRateOptions(final boolean rate,
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

    /**
     * Parses expression out of exprs, adds them to TsQuery
     * @param exprs
     * @param tsQuery
     * @param metricQueries
     * @param expressionTrees
     */
    public static void syntaxCheck(String[] exprs, TsQuery tsQuery, List<String> metricQueries, List<ExpressionTree> expressionTrees) {
        for (String expr : exprs) {
            SyntaxChecker checker = new SyntaxChecker(new StringReader(expr));
            checker.setMetricQueries(metricQueries);
            checker.setTsQuery(new TsQuery(tsQuery));

            try {
                ExpressionTree tree = checker.EXPRESSION();
                expressionTrees.add(tree);
            } catch (ParseException e) {
                throw new RuntimeException("Could not parse " + expr, e);
            }
        }
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
     * Parse out rate, downsample, aggregator from metric query and adds new query
     * into TsQuery
     * @param queryString
     * @param dataQuery
     */
    public static void parseMTypeSubQuery(final String queryString,
                                           TsQuery dataQuery) {
        if (queryString == null || queryString.isEmpty()) {
            throw new BadRequestException("The query string was empty");
        }

        // m is of the following forms:
        // agg:[interval-agg:][rate:]metric[{tag=value,...}]
        // where the parts in square brackets `[' .. `]' are optional.
        final String[] parts = SplicerUtils.splitString(queryString, ':');
        int i = parts.length;
        if (i < 2 || i > 5) {
            throw new BadRequestException("Invalid parameter m=" + queryString + " ("
                    + (i < 2 ? "not enough" : "too many") + " :-separated parts)");
        }
        final TSSubQuery sub_query = new TSSubQuery();

        // the aggregator is first
        sub_query.setAggregator(parts[0]);

        i--; // Move to the last part (the metric name).
        HashMap<String, String> tags = new HashMap<String, String>();
        sub_query.setMetric(SplicerUtils.parseWithMetric(parts[i], tags));
        sub_query.setTags(tags);

        // parse out the rate and downsampler
        for (int x = 1; x < parts.length - 1; x++) {
            if (parts[x].toLowerCase().startsWith("rate")) {
                sub_query.setRate(true);
                if (parts[x].indexOf("{") >= 0) {
                    sub_query.setRateOptions(SplicerUtils.parseRateOptions(true, parts[x]));
                }
            } else if (Character.isDigit(parts[x].charAt(0))) {
                sub_query.setDownsample(parts[x]);
            }
        }

        if (dataQuery.getQueries() == null) {
            final ArrayList<TSSubQuery> subs = new ArrayList<TSSubQuery>(1);
            dataQuery.setQueries(subs);
        }
        dataQuery.addSubQuery(sub_query);
    }

    public static TsdbResult[] flatten(List<TsdbResult[]> allResults) throws IOException
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
}
