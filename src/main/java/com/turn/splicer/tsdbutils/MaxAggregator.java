package com.turn.splicer.tsdbutils;

/**
 * Extends the Aggregator interface to provide functionality for "max" style
 * aggregators that first iterate over all data points to determine a max
 */
public interface MaxAggregator extends Aggregator {

    public long[] getLongMaxes();

    public double[] getDoubleMaxes();

    public boolean hasLongs();

    public boolean hasDoubles();
}
