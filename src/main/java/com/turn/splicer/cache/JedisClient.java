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

package com.turn.splicer.cache;

import com.turn.splicer.Config;

import javax.annotation.Nullable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author sgangam
 */
public class JedisClient {

	private static final boolean CACHE_ENABLED = Config.get().getBoolean("caching.enabled");

	protected final JedisPool jedisPool;

	private static final JedisClient CLIENT = new JedisClient();

	private JedisClient() {
		if (CACHE_ENABLED) {
			String hostPortConfig = Config.get().getString("caching.hosts");
			if (hostPortConfig == null) throw new NullPointerException("Could not find config");

			String[] hp = hostPortConfig.split(":");

			if (hp.length != 2) throw new IllegalArgumentException("Bad config for redis server");
			jedisPool = new JedisPool(new JedisPoolConfig(), hp[0], Integer.parseInt(hp[1]));

			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				@Override
				public void run() {
					jedisPool.close();
				}
			}));

		} else {
			jedisPool = null;
		}
	}

	public static JedisClient get() {
		return CLIENT;
	}

	public void put(String key, String value) {
		if (CACHE_ENABLED && jedisPool != null) {
			try (Jedis jedis = jedisPool.getResource()) {
				jedis.set(key, value);
			}
		}
	}

	@Nullable
	public String get(String key) {
		if (CACHE_ENABLED && jedisPool != null) {
			try (Jedis jedis = jedisPool.getResource()) {
				return jedis.get(key);
			}
		} else {
			return null;
		}
	}

	public String config() {
		if (CACHE_ENABLED && jedisPool != null) {
			return "running at=" + Config.get().getString("caching.hosts")
					+ ", numActive=" + jedisPool.getNumActive();
		} else {
			return "not enabled";
		}
	}

}
