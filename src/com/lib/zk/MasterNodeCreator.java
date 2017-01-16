package com.lib.zk;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public class MasterNodeCreator {

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		// TODO Auto-generated method stub
		ZookeeperHelper helper = new ZookeeperHelper("localhost",2181);
		helper.connect();
		helper.initTree(2);
		helper.addMasterDetails("192.168.0.20:9000");
		while(true);
	}

}
