package com.lib.zk;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZookeeperHelper {

	String zkHost;
	int zkPort;
	ZooKeeper zk;
	
	public ZookeeperHelper(String host, int port){
		zkHost = host;
		zkPort = port;
		//zk = new ZooKeeper();	
	}
	
	public ZooKeeper getZooKeeper(){
		return zk;
	}
	
	public void connect() throws IOException, InterruptedException{
		final CountDownLatch connSignal = new CountDownLatch(0);
		zk = new ZooKeeper(zkHost,zkPort,new Watcher()
		{
			public void process(WatchedEvent event) {
				
                if (event.getState() == KeeperState.SyncConnected) {
                	System.out.println("ZooKeeperHelper:: Connected to ZooKeeper at "+zkHost+":"+zkPort);
                    connSignal.countDown();
                }
                
            }
		}
		);
		connSignal.await();
	}
	
	public void createEphemeralNode(String path, String data) throws KeeperException, InterruptedException, UnsupportedEncodingException{
		
		byte[] b = data.getBytes("UTF-16");
		zk.create(path, b, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		System.out.println("ZooKeeperHelper:: Created Ephemeral Node at "+path);
	}
	
	public boolean createPersistentNode(String path, String data) throws KeeperException, InterruptedException, UnsupportedEncodingException{
		
		byte[] b = data.getBytes("UTF-16");
		if(zk.exists(path, false) == null){
			zk.create(path, b, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("ZooKeeperHelper:: Created Persistent Node at "+path);
			return true;
		}
		return false;
	}
	
	public void initTree(int clusterSize) throws UnsupportedEncodingException, KeeperException, InterruptedException{
		createPersistentNode(ZooKeeperUtils.PATH_NODE_ROOT, "masterRoot");
		for(int i=0;i<clusterSize;i++){
			createPersistentNode(ZooKeeperUtils.PATH_NODE_ROOT+"/"+i, "{}");
		}
	}
	
	public boolean addMasterDetails(String addressString){
		boolean result = true;
		try {
			createEphemeralNode(ZooKeeperUtils.PATH_MASTER,addressString);
		} catch (UnsupportedEncodingException e) {
			result = false;
			e.printStackTrace();
		} catch (KeeperException e) {
			result = false;
			e.printStackTrace();
		} catch (InterruptedException e) {
			result = false;
			e.printStackTrace();
		}
		return result;
	}
	
	public String getMasterDetails(){
		try {
			if(zk.exists(ZooKeeperUtils.PATH_MASTER, false) == null){
				CountDownLatch latch = new CountDownLatch(1);
				zk.exists(ZooKeeperUtils.PATH_MASTER, 
						new ZKMonitor(this,latch,ZKMonitor.MASTER_MONITOR));
				latch.await();
			} 
			byte[] b = zk.getData(ZooKeeperUtils.PATH_MASTER, false, null);
			String result = new String(b,"UTF-16");
			return result;
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
}
