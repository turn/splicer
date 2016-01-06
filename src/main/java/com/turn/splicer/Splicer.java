package com.turn.splicer;

import com.turn.splicer.tsdbutils.TsQuery;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splicer {

	private static final Logger LOG = LoggerFactory.getLogger(Splicer.class);

	public static final int SLICE_SIZE = 3600;
	public static final int OVERFLOW = 300;

	private final TsQuery tsQuery;

	public Splicer(TsQuery tsQuery) {
		this.tsQuery = tsQuery;
	}

	/**
	 * Slices a query into pieces with each with time frame atmost of {@link #SLICE_SIZE}.
	 *
	 * NOTE: It must be noted that this method does not take into consideration the downsample.
	 *
	 * What does this mean? If we have a downsample of 30m, and each slice is divided into one hour
	 * blocks, then we will only get one point per hour slice. This will reduce the output of the
	 * final query by 50%. Thus, if {@link #SLICE_SIZE} is one hour, we must add some overflow
	 * time to accomodate for these points. By default, this is set to {@link #OVERFLOW}.
	 *
	 * This can be better tuned, than hardcoding to a fixed {@link #OVERFLOW} value. As most queries
	 * use a downsample of 1-5 minutes, this would mean dropping 1 out of 60-12 points respectively.
	 *
	 * @return list of queries
	 */
	public List<TsQuery> sliceQuery()
	{
		final long bucket_size = SLICE_SIZE * 1000;

		final long overflow_in_millis;
		if (Config.get().getBoolean("slice.overflow.enable")) {
			overflow_in_millis = OVERFLOW * 1000;
		} else {
			overflow_in_millis = 0;
		}

		long startTime = tsQuery.startTime();
		long endTime = tsQuery.endTime();

		List<TsQuery> slices = new ArrayList<>();

		long end = startTime - (startTime % bucket_size) + bucket_size;
		TsQuery first = TsQuery.sliceOf(tsQuery, startTime - (startTime % bucket_size), end + overflow_in_millis);
		slices.add(first);
		LOG.debug("First interval is {} to {}", Const.tsFormat(first.startTime()), Const.tsFormat(first.endTime()));

		while (end + bucket_size < endTime) {
			TsQuery slice = TsQuery.sliceOf(tsQuery, end, end + bucket_size + overflow_in_millis);
			slices.add(slice);
			end = end + bucket_size;
			LOG.debug("Add interval# {} from {} to {}", slices.size(),
					Const.tsFormat(slice.startTime()),
					Const.tsFormat(slice.endTime()));
		}

		slices.add(TsQuery.sliceOf(tsQuery, end, endTime));
		LOG.debug("Last interval is {} to {}", Const.tsFormat(end), Const.tsFormat(endTime));

		return slices;
	}

}
