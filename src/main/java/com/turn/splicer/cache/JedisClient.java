package com.turn.splicer.cache;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * @author sgangam
 */
public class JedisClient {


	private JedisCluster jc;

	public static void main(String[] args) {
		JedisClient jc = new JedisClient();
		jc.init();

		jc.put("a", "A1");
		jc.put("b", "B1");
		jc.put("c", "C1");

		System.out.println(jc.get("a") + " " + jc.get("b") + " " + jc.get("c"));
		jc.deinit();
	}

	private void put(String key, String value) {
		this.jc.set(key, value);
	}

	private void init() {
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30001));
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30002));
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30003));
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30004));
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30005));
		jedisClusterNodes.add(new HostAndPort("127.0.0.1", 30006));
		this.jc = new JedisCluster(jedisClusterNodes);
	}

	private void deinit() {
		//NO OP
	}

	private String get(String key) {
		return this.jc.get(key);
	}


}
