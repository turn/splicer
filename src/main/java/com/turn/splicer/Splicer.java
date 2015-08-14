package com.turn.splicer;

import com.turn.splicer.tsdbutils.TsQuery;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splicer {

	private static final Logger LOG = LoggerFactory.getLogger(Splicer.class);

	private static final int SLICE_SIZE = 3600;
	private static final int OVERFLOW = 300;

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
		long startTime = tsQuery.startTime();
		long endTime = tsQuery.endTime();

		List<TsQuery> slices = new ArrayList<>();
		long end = startTime - (startTime % bucket_size) + bucket_size;
		slices.add(TsQuery.sliceOf(tsQuery, startTime - (startTime % bucket_size), end + OVERFLOW));
		LOG.info("First interval is {} to {}", startTime, end);

		while (end + bucket_size < endTime) {
			TsQuery slice = TsQuery.sliceOf(tsQuery, end, end + bucket_size + OVERFLOW);
			slices.add(slice);
			end = end + bucket_size;
			LOG.info("Add interval# {} from {} to {}", slices.size(),
					slice.startTime(),
					slice.endTime());
		}

		slices.add(TsQuery.sliceOf(tsQuery, end, endTime));
		LOG.info("Last interval is {} to {}", end, endTime);

		return slices;
	}

}
