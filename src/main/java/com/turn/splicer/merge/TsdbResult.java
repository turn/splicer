package com.turn.splicer.merge;

import com.turn.splicer.tsdbutils.DataPoint;
import com.turn.splicer.tsdbutils.MutableDataPoint;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;


public class TsdbResult {

	protected static final ObjectMapper JSON_MAPPER = (new ObjectMapperFactory()).get();

	private String metric;

	private String alias = null;

	private Tags tags;

	private List<String> aggregateTags;

	private List<String> tsuids;

	private Points dps;

	public static TsdbResult from(String jsonString) {
		try {
			return JSON_MAPPER.readValue(jsonString, TsdbResult.class);
		} catch (IOException e) {
			throw new MergeException("Could not deserialize jsonString", e);
		}
	}

	public static TsdbResult copyMeta(TsdbResult source) {
		TsdbResult copy = new TsdbResult();
		copy.metric = source.getMetric();
		copy.alias = source.getAlias();
		copy.tags = new Tags(new HashMap<String, String>(source.getTags().getTags()));
		copy.aggregateTags = source.getAggregateTags() != null ?
				new ArrayList<String>(source.getAggregateTags()) : null;
		copy.tsuids = source.getTsuids() != null ? new ArrayList<String>(source.getTsuids()) : null;
		copy.dps = new Points(new HashMap<String, Object>());
		return copy;
	}

	@Nonnull
	public static TsdbResult[] fromArray(String jsonArrayString) {
		try {
			TsdbResult[] tmp = JSON_MAPPER.readValue(jsonArrayString, TsdbResult[].class);
			if (tmp == null) tmp = new TsdbResult[]{};
			return tmp;
		} catch (IOException e) {
			throw new MergeException("Could not deserialize jsonString", e);
		}
	}

	public static String toJson(TsdbResult[] result) throws IOException {
		Preconditions.checkNotNull(result);
		Preconditions.checkArgument(result.length > 0);
		return JSON_MAPPER.writeValueAsString(result);
	}

	public String getMetric() {
		if(alias != null) {
			return alias;
		}
		return metric;
	}

	public void setMetric(String metricName) {
		this.metric = metricName;
	}

	public Tags getTags() {
		return tags;
	}

	public void setTags(Tags tags) {
		this.tags = tags;
	}

	public List<String> getAggregateTags() {
		return aggregateTags;
	}

	public void setAggregateTags(List<String> aggregateTags) {
		this.aggregateTags = aggregateTags;
	}

	public List<String> getTsuids() {
		return tsuids;
	}

	public void setTsuids(List<String> tsuids) {
		this.tsuids = tsuids;
	}

	public Points getDps() {
		return dps;
	}

	public void setDps(Points dps) {
		this.dps = dps;
	}

	public static class Points {
		Map<String, Object> map;

		public Points(Map<String, Object> m) {
			this.map = m;
		}

		public Map<String, Object> getMap() {
			return map;
		}

		public void addPoint(DataPoint dp) {
			String timestamp = String.valueOf(dp.timestamp());
			if(dp.isInteger()) {
				map.put(timestamp, dp.longValue());
			} else {
				map.put(timestamp, dp.doubleValue());
			}
		}

		/**
		 * Returns a DataPoint[] sorted by timestamp with no scaling of the values
		 * @return
		 * @throws Exception
		 */
		public DataPoint[] getDataPointsFromTreeMap() throws Exception {
			return getDataPointsFromTreeMap(1);
		}

		/**
		 * Returns a DataPoint[] - sorted by timestamp - of the Points in the
		 * map, will multiply the value of each point by the scaleFactor param
		 *
		 * uses a Treemap to get the sorting by timestamp
		 * @param scaleFactor
		 * @return
		 * @throws Exception
		 */
		public DataPoint[] getDataPointsFromTreeMap(int scaleFactor) throws Exception {
			TreeMap<String, Object> treeMap = new TreeMap(map);
			DataPoint[] dps = new DataPoint[treeMap.size()];
			int index = 0;
			for(Map.Entry<String, Object> entry: treeMap.entrySet()) {
				MutableDataPoint dp = new MutableDataPoint();
				Object val = entry.getValue();
				long timestamp = Long.valueOf(entry.getKey());
				if(val instanceof Double) {
					dp.reset(timestamp, ((Double) val).doubleValue() * scaleFactor);
				} else if(val instanceof Long) {
					dp.reset(timestamp, ((Long) val).longValue() * scaleFactor);
				} else {
					throw new Exception("Unexpected type in map: " + val.getClass());
				}
				dps[index] = dp;
				index++;
			}

			return dps;
		}

		/**
		 * Returns a DataPoint[] -sorted by timestamp - of the reciprocal of points
		 * in the Points map eg 1 / value (needed for division)
		 * @return
		 * @throws Exception
		 */
		public DataPoint[] getDataPointsFromTreeMapReciprocal() throws Exception {
			TreeMap<String, Object> treeMap = new TreeMap(map);
			DataPoint[] dps = new DataPoint[treeMap.size()];
			int index = 0;
			for(Map.Entry<String, Object> entry: treeMap.entrySet()) {
				MutableDataPoint dp = new MutableDataPoint();
				Object val = entry.getValue();
				long timestamp = Long.valueOf(entry.getKey());
				if(val instanceof Double) {
					double doubleVal = ((Double) val).doubleValue();
					if (DoubleMath.fuzzyCompare(doubleVal, 0, 1E-7) != 0) {
						dp.reset(timestamp, 1 / doubleVal);
					}
				} else if(val instanceof Long) {
					long longVal = ((Long) val).longValue();
					if(longVal != 0) {
						dp.reset(timestamp, 1 / longVal);
					}
				} else {
					throw new Exception("Unexpected type in map: " + val.getClass());
				}
				dps[index] = dp;
				index++;
			}

			return dps;
		}



		@Override
		public String toString() {
			return "Points{" +
					"map=" + map +
					'}';
		}


	}

	public static class Tags {
		private Map<String, String> tags;

		public Tags(Map<String, String> tags) {
			this.tags = tags;
		}

		public Map<String, String> getTags() {
			return tags;
		}

		@Override
		public String toString() {
			return "Tags{" +
					"tags=" + tags +
					'}';
		}
	}

	public static class TagsSerializer extends JsonSerializer<Tags> {

		@Override
		public void serialize(Tags tags, JsonGenerator jgen, SerializerProvider provider)
				throws IOException {
			if (tags.getTags() == null) {
				return;
			}

			jgen.writeStartObject();

			TreeMap<String, String> sorted = new TreeMap<>(tags.getTags());
			for (Map.Entry<String, String> e: sorted.entrySet()) {
				jgen.writeStringField(e.getKey(), e.getValue());
			}

			jgen.writeEndObject();
		}
	}

	public static class TagsDeserializer extends JsonDeserializer<Tags> {
		@Override
		public Tags deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
			TreeNode n = jp.getCodec().readTree(jp);
			Map<String, String> tags = new HashMap<>();
			Iterator<String> namesIter = n.fieldNames();
			while(namesIter.hasNext()) {
				String field = namesIter.next();
				TreeNode child = n.get(field);
				if (child instanceof TextNode) {
					tags.put(field, ((TextNode) child).asText());
				}
			}
			return new Tags(tags);
		}
	}

	public static class PointsSerializer extends JsonSerializer<Points> {

		@Override
		public void serialize(Points value, JsonGenerator jgen, SerializerProvider provider)
				throws IOException {
			if (value.getMap() == null) {
				return;
			}

			jgen.writeStartObject();
			List<String> keys = new ArrayList<>(value.getMap().keySet());
			if (!(value.getMap() instanceof TreeMap)) {
				Collections.sort(keys);
			}

			for (String key: keys) {
				Object o = value.getMap().get(key);
				if (o instanceof Integer) {
					jgen.writeNumberField(key, (Integer) o);
				} else if (o instanceof Long) {
					jgen.writeNumberField(key, (Long) o);
				} else if (o instanceof Float ) {
					jgen.writeNumberField(key, (Float) o);
				} else if (o instanceof Double) {
					jgen.writeNumberField(key, (Double) o);
				}
			}

			jgen.writeEndObject();
		}
	}

	public static class PointsDeserializer extends JsonDeserializer<Points> {
		@Override
		public Points deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
			TreeNode n = jp.getCodec().readTree(jp);
			Map<String, Object> points = new HashMap<>();
			Iterator<String> namesIter = n.fieldNames();
			while(namesIter.hasNext()) {
				String field = namesIter.next();
				TreeNode child = n.get(field);

				Object o;
				if (child instanceof DoubleNode || child instanceof FloatNode) {
					o = ((NumericNode) child).doubleValue();
				} else if (child instanceof IntNode || child instanceof LongNode) {
					o = ((NumericNode) child).longValue();
				} else {
					throw new MergeException("Unsupported Type, " + child.getClass());
				}

				points.put(field, o);
			}
			return new Points(points);
		}
	}

	public String getAlias() { return alias;}

	public void setAlias(String alias) {this.alias = alias;}

	@Override
	public String toString() {
		return "TsdbResult{" +
				"metric='" + metric + '\'' +
				", tags=" + tags +
				", aggregateTags=" + aggregateTags +
				", tsuids=" + tsuids +
				", dps=" + dps +
				'}';
	}

	static class ObjectMapperFactory {

		public ObjectMapper get() {
			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

			SimpleModule module = new SimpleModule("SliceResultsDeserializerModule");
			module.addSerializer(Points.class, new PointsSerializer());
			module.addSerializer(Tags.class, new TagsSerializer());
			module.addDeserializer(Points.class, new PointsDeserializer());
			module.addDeserializer(Tags.class, new TagsDeserializer());
			mapper.registerModule(module);

			return mapper;
		}
	}

}
