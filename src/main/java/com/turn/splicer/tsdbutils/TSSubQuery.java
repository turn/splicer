package com.turn.splicer.tsdbutils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TSSubQuery {
	/**
	 * User given name of an aggregation function to use
	 */
	private String aggregator;

	/**
	 * User given name for a metric, e.g. "sys.cpu.0"
	 */
	private String metric;

	/**
	 * User provided list of timeseries UIDs
	 */
	private List<String> tsuids;

	/**
	 * User supplied list of tags for specificity or grouping. May be null or
	 * empty
	 */
	private HashMap<String, String> tags;

	/**
	 * User given downsampler
	 */
	private String downsample;

	/**
	 * Whether or not the user wants to perform a rate conversion
	 */
	private boolean rate;

	/**
	 * Rate options for counter rollover/reset
	 */
	private RateOptions rate_options;

	/**
	 * Parsed downsample interval
	 */
	private long downsample_interval;

	/**
	 * Default constructor necessary for POJO de/serialization
	 */
	public TSSubQuery() {

	}

	public String toString() {
		final StringBuilder buf = new StringBuilder();
		buf.append("TSSubQuery(metric=")
				.append(metric == null || metric.isEmpty() ? "" : metric);
		buf.append(", tags=[");
		if (tags != null && !tags.isEmpty()) {
			int counter = 0;
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				if (counter > 0) {
					buf.append(", ");
				}
				buf.append(entry.getKey())
						.append("=")
						.append(entry.getValue());
				counter++;
			}
		}
		buf.append("], tsuids=[");
		if (tsuids != null && !tsuids.isEmpty()) {
			int counter = 0;
			for (String tsuid : tsuids) {
				if (counter > 0) {
					buf.append(", ");
				}
				buf.append(tsuid);
				counter++;
			}
		}
		buf.append("], agg=")
				.append(aggregator)
				.append(", downsample=")
				.append(downsample)
				.append(", ds_interval=")
				.append(downsample_interval)
				.append(", rate=")
				.append(rate)
				.append(", rate_options=")
				.append(rate_options);
		buf.append(")");
		return buf.toString();
	}

	/**
	 * Runs through query parameters to make sure it's a valid request.
	 * This includes parsing the aggregator, downsampling info, metrics, tags or
	 * timeseries and setting the local parsed fields needed by the TSD for proper
	 * execution. If no exceptions are thrown, the query is considered valid.
	 * <b>Note:</b> You do not need to call this directly as it will be executed
	 * by the {@link TsQuery} object the sub query is assigned to.
	 *
	 * @throws IllegalArgumentException if something is wrong with the query
	 */
	public void validateAndSetQuery() {
		if (aggregator == null || aggregator.isEmpty()) {
			throw new IllegalArgumentException("Missing the aggregation function");
		}

		// we must have at least one TSUID OR a metric
		if ((tsuids == null || tsuids.isEmpty()) &&
				(metric == null || metric.isEmpty())) {
			throw new IllegalArgumentException(
					"Missing the metric or tsuids, provide at least one");
		}
	}

	/**
	 * @return whether or not the user requested a rate conversion
	 */
	public boolean getRate() {
		return rate;
	}

	/**
	 * @return options to use for rate calculations
	 */
	public RateOptions getRateOptions() {
		return rate_options;
	}

	/**
	 * @return the parsed downsample interval in seconds
	 */
	public long downsampleInterval() {
		return this.downsample_interval;
	}

	/**
	 * @return the user supplied aggregator
	 */
	public String getAggregator() {
		return aggregator;
	}

	/**
	 * @return the user supplied metric
	 */
	public String getMetric() {
		return metric;
	}

	/**
	 * @return the user supplied list of TSUIDs
	 */
	public List<String> getTsuids() {
		return tsuids;
	}

	/**
	 * @return the raw downsampling function request from the user,
	 * e.g. "1h-avg"
	 */
	public String getDownsample() {
		return downsample;
	}

	/**
	 * @return the user supplied list of query tags, may be empty
	 */
	public Map<String, String> getTags() {
		if (tags == null) {
			return Collections.emptyMap();
		}
		return tags;
	}

	/**
	 * @param aggregator the name of an aggregation function
	 */
	public void setAggregator(String aggregator) {
		this.aggregator = aggregator;
	}

	/**
	 * @param metric the name of a metric to fetch
	 */
	public void setMetric(String metric) {
		this.metric = metric;
	}

	/**
	 * @param tsuids a list of timeseries UIDs as hex encoded strings to fetch
	 */
	public void setTsuids(List<String> tsuids) {
		this.tsuids = tsuids;
	}

	/**
	 * @param tags an optional list of tags for specificity or grouping
	 */
	public void setTags(HashMap<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * @param downsample the downsampling function to use, e.g. "2h-avg"
	 */
	public void setDownsample(String downsample) {
		this.downsample = downsample;
	}

	/**
	 * @param rate whether or not the result should be rate converted
	 */
	public void setRate(boolean rate) {
		this.rate = rate;
	}

	/**
	 * @param options Options to set when calculating rates
	 */
	public void setRateOptions(RateOptions options) {
		this.rate_options = options;
	}

}