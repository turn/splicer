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

package com.turn.splicer;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;

/**
 * Implements the suggest api for requests with api/suggest by passing the
 * request on to a TSDB node and returning the result
 * for example: TSDB_HOST/api/suggest?type=metrics&q=tcollector"
 * should give you the first 25 metrics that start with "tcollector"
 *
 * Created by bpeltz on 10/19/15.
 */
public class SuggestServlet extends HttpServlet {

	private static final Logger LOG = LoggerFactory.getLogger(SuggestServlet.class);

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		try {
			SuggestHttpWorker worker = new SuggestHttpWorker(request.getQueryString());
			String json = worker.call();

			response.setContentType("application/json");
			response.getWriter().write(json);
			response.getWriter().flush();
		} catch (IOException e) {
			LOG.error("IOException which processing GET request", e);
		} catch (Exception e) {
			LOG.error("Exception which processing GET request", e);
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		//ok let's see if I can build the query string out of the request object
		BufferedReader reader = request.getReader();
		String line;
		StringBuilder builder = new StringBuilder();
		while ((line = reader.readLine()) != null) {
			builder.append(line);
		}

		String jsonPostRequest = builder.toString();

		JSONObject obj = null;

		try {
			obj = new JSONObject(jsonPostRequest);
		} catch (JSONException e) {
			LOG.error("Exception reading POST data " + jsonPostRequest);
			response.getWriter().write("Exception reading POST data " + jsonPostRequest);
			return;
		}

		String type = null;

		try {
			type = obj.getString("type");
		} catch (JSONException e) {
			LOG.error("No type provided for suggest request " + jsonPostRequest);
			response.getWriter().write("No type provided for suggest request " + jsonPostRequest);
			return;
		}

		StringBuilder getRequestString = new StringBuilder();
		getRequestString.append("type=");
		getRequestString.append(type);

		String q = null;
		Integer max = null;
		//q is optional
		try {
			q = obj.getString("q");
		} catch (JSONException e) {}

		//max is optional
		try {
			max = obj.getInt("max");
		} catch (JSONException e) {}

		if (q != null) {
			getRequestString.append("&q=");
			getRequestString.append(q);
		}

		if (max != null) {
			getRequestString.append("&max=");
			getRequestString.append(max);
		}
		String requestString = getRequestString.toString();

		SuggestHttpWorker worker = new SuggestHttpWorker(requestString);
		String json = null;
		try {
			json = worker.call();
		} catch (Exception e) {
			LOG.error("Exception processing POST reqeust", e);
			response.getWriter().write("Exception processing POST request" + e);
		}

		response.setContentType("application/json");
		response.getWriter().write(json);
		response.getWriter().flush();
	}
}
