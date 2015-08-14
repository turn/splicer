package com.turn.splicer.cache;

import com.turn.splicer.Config;

import javax.annotation.Nullable;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author sgangam
 */
public class JedisClient {

	private static final boolean CACHE_ENABLED = Config.get().getBoolean("caching.enabled");

	//The JedisCluster is threadsafe
	private static final HostAndPort[] hostAndPorts = {
			new HostAndPort("127.0.0.1", 30001),
			new HostAndPort("127.0.0.1", 30002),
			new HostAndPort("127.0.0.1", 30003),
			new HostAndPort("127.0.0.1", 30004),
			new HostAndPort("127.0.0.1", 30005),
			new HostAndPort("127.0.0.1", 30006)
	};

	private static final JedisClient CLIENT = new JedisClient();

	private final JedisCluster jedisCluster;

	private JedisClient() {
		if (CACHE_ENABLED) {
			jedisCluster = new JedisCluster(new HashSet<>(Arrays.asList(hostAndPorts)));
		} else {
			jedisCluster = null;
		}
	}

	public static JedisClient get() {
		return CLIENT;
	}

	public void put(String key, String value) {
		if (CACHE_ENABLED && jedisCluster != null) {
			jedisCluster.set(key, value);
		}
	}

	@Nullable
	public String get(String key) {
		if (CACHE_ENABLED && jedisCluster != null) {
			return jedisCluster.get(key);
		} else {
			return null;
		}
	}

	public static void main(String[] args) {
		JedisClient jc = new JedisClient();

		jc.put("a", "A1");
		jc.put("b", "B1");
		jc.put("c", "C1");

		System.out.println(jc.get("a") + " " + jc.get("b") + " " + jc.get("c"));
	}

}
