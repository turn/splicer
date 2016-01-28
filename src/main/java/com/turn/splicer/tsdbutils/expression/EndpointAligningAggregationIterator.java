/**
 * Copyright 2015-2016 The Splicer Query Engine Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.turn.splicer.tsdbutils.expression;

import com.turn.splicer.tsdbutils.Aggregator;
import com.turn.splicer.tsdbutils.Aggregators;
import com.turn.splicer.tsdbutils.SeekableView;

/**
 * Identical to AggregationIterator except for how it handles edges of a Span.
 *
 *	In the constructor it will skip ahead to a point where each span has a nonzero
 * value for the next call to next().
 *
 * 	In hasNext() it will return false if any span is out of points.
 *
 * 	This solves the problem with the difference function, where diff(A, B)
 * 	was producing spiking values if B did not have values for timestamps that
 *  A had values for around the start and end time of a query.  This caused us to
 *  get a value diff(A, 0) which would set off alerts unnecessarily.
 *  The solution was to shave off points at the boundary where one span didn't have values.
 *
 *  @author Brian Peltz
 */
public class EndpointAligningAggregationIterator extends AggregationIterator {

	public EndpointAligningAggregationIterator(SeekableView[] iterators, long start_time, long end_time, Aggregator aggregator, Aggregators.Interpolation method, boolean rate) {
		super(iterators, start_time, end_time, aggregator, method, rate);

		alignFirstTimestamps();
	}

	/**
	 * Goes through timestamp array looking for 0 values.  If it finds a 0 it
	 * checks to see if the next timestamp for that Span is the lowest of all next
	 * timestamps (meaning it will not be zero after the first call to next()).  If it
	 * is not the lowest it calls next() until it is the lowest. Does this until every zero
	 * value has the minimum next timestamp.
	 */
	private void alignFirstTimestamps() {
		int numSeries = iterators.length;
		//check for zeroes
		for(int i = 0; i < numSeries; i++) {
			if(timestamps[i] == 0) {
				long minTime = nextMinimumTimestamp();
				//if next timestamp for this span is not the minimum
				while(timestamps[i + numSeries] > minTime) {
					if(hasNext()) {
						next();
					} else {
						//no more data points so we're sunk
						break;
					}
					//recalculate min time
					minTime = nextMinimumTimestamp();
				}
			}
		}
	}

	private long nextMinimumTimestamp() {
		//set min as first timestamp
		long minTime = timestamps[iterators.length];
		for(int j = iterators.length; j < timestamps.length; j++) {
			if(timestamps[j] < minTime) {
				minTime = timestamps[j];
			}
		}
		return minTime;
	}

	/**
	 * Modified from AggregationIterator
	 * If any next timestamp is greater than end_time we return false,
	 * so if ANY series is out of DataPoints we don't produce values
	 * @return
	 */
	@Override
	public boolean hasNext() {
		final int size = iterators.length;

		for (int i = 0; i < size; i++) {
			// As long as any of the iterators has a data point with a timestamp
			// that falls within our interval, we know we have at least one next.
			if ((timestamps[size + i] & TIME_MASK) > end_time) {
				LOG.debug("Total aggregationTime=" + (aggregationTimeInNanos / (1000 * 1000)) + "ms.");
				LOG.debug("Total interpolationTime=" + (interpolationTimeInNanos / (1000 * 1000)) + "ms.");
				LOG.debug("Total downSampleTime=" + (downsampleTimeInNanos / (1000 * 1000)) + "ms.");
				LOG.debug("Total moveToNextTime=" + (moveToNextTimeInNanos / (1000 * 1000)) + "ms.");
				//LOG.debug("No hasNext (return false)");
				return false;
			}
		}
		return true;
	}
}
