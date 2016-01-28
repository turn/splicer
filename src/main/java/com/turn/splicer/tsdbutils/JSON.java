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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

public class JSON {

	/**
	 * Jackson de/serializer initialized, configured and shared
	 */
	private static final ObjectMapper jsonMapper = new ObjectMapper();

	static {
		// allows parsing NAN and such without throwing an exception. This is
		// important
		// for incoming data points with multiple points per put so that we can
		// toss only the bad ones but keep the good
		jsonMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
	}

	public static <T> T parseToObject(final String json,
	                                        final Class<T> pojo) {
		Preconditions.checkNotNull(json);
		Preconditions.checkArgument(!json.isEmpty(), "Incoming data was null or empty");
		Preconditions.checkNotNull(pojo);

		try {
			return jsonMapper.readValue(json, pojo);
		} catch (IOException e) {
			throw new JSONException(e);
		}
	}

	/**
	 * Serializes the given object to a JSON string
	 *
	 * @param object The object to serialize
	 * @return A JSON formatted string
	 * @throws IllegalArgumentException if the object was null
	 * @throws JSONException            if the object could not be serialized
	 * @throws IOException              Thrown when there was an issue reading the object
	 */
	public static String serializeToString(Object object) {
		if (object == null)
			throw new IllegalArgumentException("Object was null");
		try {
			return jsonMapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new JSONException(e);
		}
	}

	/**
	 * Serializes the given object to a JSON byte array
	 *
	 * @param object The object to serialize
	 * @return A JSON formatted byte array
	 * @throws IllegalArgumentException if the object was null
	 * @throws JSONException            if the object could not be serialized
	 * @throws IOException              Thrown when there was an issue reading the object
	 */
	public static byte[] serializeToBytes(Object object) {
		if (object == null)
			throw new IllegalArgumentException("Object was null");
		try {
			return jsonMapper.writeValueAsBytes(object);
		} catch (JsonProcessingException e) {
			throw new JSONException(e);
		}
	}
}
