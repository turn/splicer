package com.turn.splicer.cache;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author sgangam
 */
public class JedisClient {


	//The JedisCluster is threadsafe
	private static final HostAndPort[] hostAndPorts = {
			new HostAndPort("127.0.0.1", 30001),
			new HostAndPort("127.0.0.1", 30002),
			new HostAndPort("127.0.0.1", 30003),
			new HostAndPort("127.0.0.1", 30004),
			new HostAndPort("127.0.0.1", 30005),
			new HostAndPort("127.0.0.1", 30006)
	};
	private static final JedisCluster JEDIS_CLUSTER = new JedisCluster(
			new HashSet<HostAndPort>(Arrays.asList(hostAndPorts)));

	public static void main(String[] args) {
		JedisClient jc = new JedisClient();

		jc.put("a", "A1");
		jc.put("b", "B1");
		jc.put("c", "C1");

		System.out.println(jc.get("a") + " " + jc.get("b") + " " + jc.get("c"));
	}

	public void put(String key, String value) {
		JEDIS_CLUSTER.set(key, value);
	}

	public String get(String key) {
		return JEDIS_CLUSTER.get(key);
	}


}
