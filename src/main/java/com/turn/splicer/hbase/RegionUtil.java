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

package com.turn.splicer.hbase;

import com.turn.splicer.Config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class RegionUtil {

	private static String HBASE_ZK = Config.get().getString("hbase.zookeeper.quorum");
	private static String HBASE_ZNODE_PARENT = Config.get().getString("hbase.znode.parent");

	protected Configuration config = HBaseConfiguration.create();

	public RegionUtil() {
		init();
	}

	protected void init() {
		config.set("hbase.zookeeper.quorum", HBASE_ZK);
		config.set("zookeeper.znode.parent", HBASE_ZNODE_PARENT);
	}

	public RegionChecker getRegionChecker() {
		return new RegionChecker(config);
	}

	public Configuration getConfig() {
		return config;
	}

}
