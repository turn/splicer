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
