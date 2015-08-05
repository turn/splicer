package com.turn.splicer.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class RegionUtil {

	protected Configuration config = HBaseConfiguration.create();

	public RegionUtil() {
		init();
	}

	protected void init() {
		config.set("hbase.zookeeper.quorum", "dwh-head001.atl1.turn.com,dwh-head002.atl1.turn.com,dwh-head003.atl1.turn.com");
		config.set("zookeeper.znode.parent", "/hbase-unsecure");
	}

	public RegionChecker getRegionChecker() {
		return new RegionChecker(config);
	}

	public Configuration getConfig() {
		return config;
	}

}
