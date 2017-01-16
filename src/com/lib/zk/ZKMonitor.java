package com.lib.zk;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ZKMonitor implements Watcher {
	
	ZookeeperHelper zk;
	CountDownLatch latch;
	int monitor;
	ZkPayload packet;
	//HashSet<String> dataSet;
	//HashMap<String,V> dataMap;
	
	public static final int CHILDREN_MONITOR = 1;
	public static final int MASTER_MONITOR = 2;
	
	public ZKMonitor(ZookeeperHelper z, CountDownLatch l, int monitorType){
		zk = z;
		latch = l;
		monitor = monitorType;
	}
	
	
	public ZKMonitor(ZookeeperHelper z, CountDownLatch l, int monitorType, ZkPayload packet){
		zk = z;
		latch = l;
		monitor = monitorType;
		this.packet = packet;
	}
	/*
	public ZKMonitor(ZookeeperHelper z, CountDownLatch l, int monitorType, HashMap<String,V> lst){
		zk = z;
		latch = l;
		monitor = monitorType;
		
	}
	*/
	@Override
	public void process(WatchedEvent event) {
		switch(event.getType()){
		case NodeChildrenChanged:
			break;
		case NodeCreated:
			break;
		case NodeDeleted:
			break;
		default:
			break;
		}
		System.out.println("ZkMonitor:: Watch Event observed of type"+event.getType());
		latch.countDown();
	}
}
