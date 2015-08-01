package com.turn.splicer;

import com.turn.splicer.tsdbutils.TsQuery;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splicer {

	private static final Logger LOG = LoggerFactory.getLogger(Splicer.class);

	private static final int SLICE_SIZE = 3600;

	private final TsQuery tsQuery;

	public Splicer(TsQuery tsQuery) {
		this.tsQuery = tsQuery;
	}

	public List<TsQuery> sliceQuery() {
		final long bucket_size = SLICE_SIZE * 1000;
		long startTime = tsQuery.startTime();
		long endTime = tsQuery.endTime();

		List<TsQuery> slices = new ArrayList<>();
		long end = startTime - (startTime % bucket_size) + bucket_size;
		slices.add(TsQuery.sliceOf(tsQuery, startTime - (startTime % bucket_size), end + 100));
		LOG.info("First interval is {} to {}", startTime, end);

		while (end + bucket_size < endTime) {
			TsQuery slice = TsQuery.sliceOf(tsQuery, end, end + bucket_size + (100));
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
