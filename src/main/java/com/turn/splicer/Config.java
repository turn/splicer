package com.turn.splicer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {

	private static final Logger LOG = LoggerFactory.getLogger(Config.class);

	private static final String CONFIG_FILE = "splicer.conf";

	private static final String VERSION_FILE = "VERSION";

	private static Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

	private static final Config INSTANCE = new Config();

	protected Properties properties = new Properties();

	private Config() {
		try {
			InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE);
			if (is != null) {
				LOG.info("Loaded {} bytes of configuration", is.available());
			}

			properties.load(is);
		} catch (IOException e) {
			LOG.error("Could not load " + CONFIG_FILE, e);
		}
	}

	public static Config get() {
		return INSTANCE;
	}

	/**
	 * Read an integer from the config file
	 * @param field property name
	 * @return integer or throws NumberFormatException if property doesn't
	 *         exist or is not an integer
	 */
	public int getInt(String field) {
		return Integer.parseInt(properties.getProperty(field));
	}

	public boolean getBoolean(String field) {
		return Boolean.parseBoolean(properties.getProperty(field));
	}

	/**
	 * Read a string from the config file
	 * @param field property name
	 * @return string or null if property doesn't exist
	 */
	public String getString(String field) {
		return properties.getProperty(field);
	}

	/**
	 * For properties of the form:
	 *
	 * key = value1,value2,value3....valuek
	 * this will return an iterable of the values.
	 *
	 * @param field property name
	 * @return list of (value1, value2... valuek)
	 */
	public Iterable<String> getStrings(String field) {
		String all = getString(field);
		return COMMA_SPLITTER.split(all);
	}

	public void writeAsJson(JsonGenerator jgen) throws IOException
	{
		if (properties == null) {
			jgen.writeStartObject();
			jgen.writeEndObject();
			return;
		}

		TreeMap<String, String> map = new TreeMap<>();
		for (Map.Entry<Object, Object> e: properties.entrySet()) {
			map.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
		}

		InputStream is = getClass().getClassLoader().getResourceAsStream(VERSION_FILE);
		if (is != null) {
			LOG.debug("Loaded {} bytes of version file configuration", is.available());
			Properties versionProps = new Properties();
			versionProps.load(is);
			for (Map.Entry<Object, Object> e: versionProps.entrySet()) {
				map.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
			}
		}  else {
			LOG.error("No version file found on classpath. VERSION_FILE={}", VERSION_FILE);
		}

		jgen.writeStartObject();
		for (Map.Entry<String, String> e: map.entrySet()) {
			if (e.getValue().indexOf(',') > 0) {
				splitList(e.getKey(), e.getValue(), jgen);
			} else {
				jgen.writeStringField(e.getKey(), e.getValue());
			}
		}
		jgen.writeEndObject();
	}

	private void splitList(String key, String value, JsonGenerator jgen) throws IOException {
		jgen.writeArrayFieldStart(key);
		for (String o: value.split(",")) {
			jgen.writeString(o);
		}
		jgen.writeEndArray();
	}

}
