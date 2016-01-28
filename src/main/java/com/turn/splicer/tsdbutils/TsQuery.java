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

package com.turn.splicer.tsdbutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public class TsQuery {

	/**
	 * User given start date/time, could be relative or absolute
	 */
	private String start;

	/**
	 * User given end date/time, could be relative, absolute or empty
	 */
	private String end;

	/**
	 * User's timezone used for converting absolute human readable dates
	 */
	private String timezone;

	/**
	 * Options for serializers, graphs, etc
	 */
	private HashMap<String, ArrayList<String>> options;

	/**
	 * Whether or not to include padding, i.e. data to either side of the start/
	 * end dates
	 */
	private boolean padding;

	/**
	 * Whether or not to suppress annotation output
	 */
	private boolean no_annotations;

	/**
	 * Whether or not to scan for global annotations in the same time range
	 */
	private boolean with_global_annotations;

	/**
	 * Whether or not to show TSUIDs when returning data
	 */
	private boolean show_tsuids;

	/**
	 * A list of parsed sub queries, must have one or more to fetch data
	 */
	private ArrayList<TSSubQuery> queries;

	/**
	 * The parsed start time value
	 * <b>Do not set directly</b>
	 */
	private long start_time;

	/**
	 * The parsed end time value
	 * <b>Do not set directly</b>
	 */
	private long end_time;

	/**
	 * Whether or not the user wasn't millisecond resolution
	 */
	private boolean ms_resolution;

	/**
	 * Default constructor necessary for POJO de/serialization
	 */
	public TsQuery() {

	}

	/**
	 * Shallow copy constructor
	 * start, end, timezone, are Strings so those are immutable
	 */
	public TsQuery(TsQuery old) {
		this.start = old.start;
		this.end = old.end;
		this.timezone = old.timezone;
		this.options = old.options;
		this.padding = old.padding;
		this.no_annotations = old.padding;
		this.with_global_annotations = old.with_global_annotations;
		this.show_tsuids = old.show_tsuids;
		this.queries = old.queries;
		this.ms_resolution = old.ms_resolution;

		//start and end must be set with call to validate times
		this.validateTimes();
	}


	/**
	 * Runs through query parameters to make sure it's a valid request.
	 * This includes parsing relative timestamps, verifying that the end time is
	 * later than the start time (or isn't set), that one or more metrics or
	 * TSUIDs are present, etc. If no exceptions are thrown, the query is
	 * considered valid.
	 * <b>Warning:</b> You must call this before passing it on for processing as
	 * it sets the {@code start_time} and {@code end_time} fields as well as
	 * sets the {@link TSSubQuery} fields necessary for execution.
	 *
	 * @throws IllegalArgumentException if something is wrong with the query
	 */
	public void validateAndSetQuery() {
		validateTimes();

		if (queries == null || queries.isEmpty()) {
			throw new IllegalArgumentException("Missing queries");
		}

		// validate queries
		for (TSSubQuery sub : queries) {
			sub.validateAndSetQuery();
		}
	}


	/**
	 * More duplicated code. This is copied from the above functions.
	 */
	public void validateTimes() {
		if (start == null || start.isEmpty()) {
			throw new IllegalArgumentException("Missing start time");
		}
		start_time = DateTime.parseDateTimeString(start, timezone);

		if (end != null && !end.isEmpty()) {
			end_time = DateTime.parseDateTimeString(end, timezone);
		} else {
			end_time = System.currentTimeMillis();
		}
		if (end_time <= start_time) {
			throw new IllegalArgumentException(
					"End time [" + end_time + "] must be greater than the start time ["
							+ start_time + "]");
		}
	}

	/**
	 * Sets the start time for further parsing. This can be an absolute or
	 * relative value. See {@link DateTime#parseDateTimeString} for details.
	 *
	 * @param start A start time from the user
	 */
	public void setStart(String start) {
		this.start = start;
	}

	/**
	 * Optionally sets the end time for all queries. If not set, the current
	 * system time will be used. This can be an absolute or relative value. See
	 * {@link DateTime#parseDateTimeString} for details.
	 *
	 * @param end An end time from the user
	 */
	public void setEnd(String end) {
		this.end = end;
	}

	/**
	 * @param timezone an optional timezone for date parsing
	 */
	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}

	/**
	 * @param options a map of options to pass on to the serializer
	 */
	public void setOptions(HashMap<String, ArrayList<String>> options) {
		this.options = options;
	}

	/**
	 * @param padding whether or not the query should include padding
	 */
	public void setPadding(boolean padding) {
		this.padding = padding;
	}

	/**
	 * @param no_annotations whether or not to suppress annotation output
	 */
	public void setNoAnnotations(boolean no_annotations) {
		this.no_annotations = no_annotations;
	}

	/**
	 * @param with_global whether or not to load global annotations
	 */
	public void setGlobalAnnotations(boolean with_global) {
		with_global_annotations = with_global;
	}

	/**
	 * @param show_tsuids whether or not to show TSUIDs in output
	 */
	public void setShowTSUIDs(boolean show_tsuids) {
		this.show_tsuids = show_tsuids;
	}

	/**
	 * @param queries a list of {@link TSSubQuery} objects to store
	 */
	public void setQueries(ArrayList<TSSubQuery> queries) {
		this.queries = queries;
	}

	/**
	 * @param ms_resolution whether or not the user wants millisecond resolution
	 */
	public void setMsResolution(boolean ms_resolution) {
		this.ms_resolution = ms_resolution;
	}

	/**
	 * @return the parsed start time for all queries
	 */
	public long startTime() {
		return this.start_time;
	}

	/**
	 * @return the parsed end time for all queries
	 */
	public long endTime() {
		return this.end_time;
	}

	/**
	 * @return the user given, raw start time
	 */
	public String getStart() {
		return start;
	}

	/**
	 * @return the user given, raw end time
	 */
	public String getEnd() {
		return end;
	}

	/**
	 * @return the user supplied timezone
	 */
	public String getTimezone() {
		return timezone;
	}

	/**
	 * @return a map of serializer options
	 */
	public Map<String, ArrayList<String>> getOptions() {
		return options;
	}

	/**
	 * @return whether or not the user wants padding
	 */
	public boolean getPadding() {
		return padding;
	}

	/**
	 * @return whether or not to supress annotatino output
	 */
	public boolean getNoAnnotations() {
		return no_annotations;
	}

	/**
	 * @return whether or not to load global annotations for the time range
	 */
	public boolean getGlobalAnnotations() {
		return with_global_annotations;
	}

	/**
	 * @return whether or not to display TSUIDs with the results
	 */
	public boolean getShowTSUIDs() {
		return show_tsuids;
	}

	/**
	 * @return the list of sub queries
	 */
	public List<TSSubQuery> getQueries() {
		return queries;
	}

	public void addSubQuery(TSSubQuery subQuery) {
		if (queries == null) {
			queries = new ArrayList<>();
		}
		queries.add(subQuery);
	}

	/**
	 * @return whether or not the requestor wants millisecond resolution
	 */
	public boolean getMsResolution() {
		return ms_resolution;
	}


	@Override
	public String toString() {
		return "TsQuery{" +
				"start='" + start + '\'' +
				", end='" + end + '\'' +
				", timezone='" + timezone + '\'' +
				", options=" + options +
				", padding=" + padding +
				", no_annotations=" + no_annotations +
				", with_global_annotations=" + with_global_annotations +
				", show_tsuids=" + show_tsuids +
				", queries=" + queries +
				", start_time=" + start_time +
				", end_time=" + end_time +
				", ms_resolution=" + ms_resolution +
				'}';
	}

	/**
	 * Create a shallow copy of query object. Object returned is validated if the input was valid.
	 * @param query object to copy
	 * @return a shallow copy
	 */
	public static TsQuery validCopyOf(TsQuery query) {
		TsQuery tsQuery = new TsQuery();
		tsQuery.start = query.start;
		tsQuery.end = query.end;
		tsQuery.timezone = query.timezone;
		tsQuery.options = query.options;
		tsQuery.padding = query.padding;
		tsQuery.no_annotations = query.no_annotations;
		tsQuery.with_global_annotations = query.with_global_annotations;
		tsQuery.show_tsuids = query.show_tsuids;
		tsQuery.start_time = query.start_time;
		tsQuery.end_time = query.end_time;
		tsQuery.ms_resolution = query.ms_resolution;
		tsQuery.queries = new ArrayList<>();
		return tsQuery;
	}

	/**
	 * Makes a shallow copy of the tsQuery. Shallow in the senes that you need to
	 * call {@link TsQuery#validateAndSetQuery()} yourself on the returned copy
	 *
	 * @param tsQuery template to copy
 	 * @return the shallow copy of the template
	 */
	public static TsQuery copyOf(TsQuery tsQuery) {
		String data = JSON.serializeToString(tsQuery);
		return JSON.parseToObject(data, TsQuery.class);
	}

	/**
	 * Makes a copy and sets the start time and end times for the copy.
	 * This method also calls {@link TsQuery#validateAndSetQuery()} on the copy
	 *
	 * @param tsQuery the template to copy
	 * @param startTime  the start time for the copy
	 * @param endTime    the end time for the copy
	 * @return the validated query copy
	 */
	public static TsQuery sliceOf(TsQuery tsQuery, long startTime, long endTime) {
		TsQuery copy = copyOf(tsQuery);
		copy.start = String.valueOf(startTime);
		copy.end = String.valueOf(endTime);
		copy.validateAndSetQuery();
		return copy;
	}
}
