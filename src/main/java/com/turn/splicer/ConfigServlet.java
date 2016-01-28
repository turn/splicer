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

import com.turn.splicer.hbase.RegionUtil;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigServlet extends HttpServlet {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigServlet.class);

	public static RegionUtil REGION_UTIL = new RegionUtil();

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		try {
			doGetWork(request, response);
		} catch (IOException e) {
			LOG.error("IOException which processing GET request", e);
		} catch (Exception e) {
			LOG.error("Exception which processing GET request", e);
		}
	}

	private void doGetWork(HttpServletRequest request, HttpServletResponse response)
			throws IOException
	{
		response.setContentType("application/json");
		JsonGenerator generator = new JsonFactory()
				.createGenerator(response.getOutputStream(), JsonEncoding.UTF8);
		Config.get().writeAsJson(generator);
		generator.close();
	}

}
