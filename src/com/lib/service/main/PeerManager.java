package com.lib.service.main;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.lib.zk.ZKMonitor;
import com.lib.zk.ZooKeeperUtils;
import com.lib.zk.ZookeeperHelper;

public class PeerManager implements Runnable {

	ZookeeperHelper zk;
	int groupId;
	String path;
	HashSet<String> peerIds;
	HashMap<Integer,String> peerIdMap;
	StorageNode node;
	int nodeId;
	
	public PeerManager(ZookeeperHelper zk, int groupId, int nodeId, StorageNode node){
		this.zk = zk;
		this.groupId = groupId;
		this.path = ZooKeeperUtils.PATH_NODE_ROOT+"/"+groupId;
		peerIds = new HashSet<String>();
		peerIdMap = new HashMap<Integer,String>();
		this.node = node;
		this.nodeId = nodeId;
		try {
			zk.createPersistentNode("/connections", "Connection Helper");
		} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		updatePeers();
		new Thread(this).start();
	}

	public synchronized void updatePeers(){
		try {
			List<String> children = zk.getZooKeeper().getChildren(path, false);
			Set<Integer> childNodes = new HashSet<Integer>(peerIdMap.keySet());
			for(String child: children){
				//String childPath = path+"/"+child;
				int peerId = Integer.parseInt(child);
				if(child.equals(Integer.toString(nodeId)))
					continue;
				System.out.println("Discovered new peer in group "+groupId +" child: "+child);
				if( !childNodes.contains(peerId) ){
					//new node detected. Read its details and save
					
					if(nodeId < peerId){
						connectToPeer(peerId);
					} else {
						waitForConnection(peerId);
					}
					
					// push application data
				} else {
					childNodes.remove(Integer.parseInt(child));
				}
			}
			
			for(Integer node : childNodes){
				// all nodes here need to be removed
				String x = peerIdMap.get(node);
				peerIds.remove(x);
				peerIdMap.remove(node);
				System.out.println("Node disconnected: "+node);
				// trigger replication here
			}
			
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {
		System.out.println("Looking for peers");
		while(true){
			CountDownLatch latch = new CountDownLatch(1);
			try {
				zk.getZooKeeper().getChildren(path, 
						new ZKMonitor(zk,latch,ZKMonitor.CHILDREN_MONITOR));
				latch.await();
				updatePeers();
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}	
	
	public List<String> getAllPeers(){
		List<String> peers = new ArrayList<String>(peerIds.size());
		for(String peer : peerIds){
			peers.add(peer);
		}
		return peers;
	}
	
	public String getPeerAddress(Integer nodeId){
		if( peerIdMap.containsKey(nodeId))
			return peerIdMap.get(nodeId);
		return null;
	}
	
	public boolean isFull(String path){
		boolean isFullPrior = false;
		byte[] b;
		try {
			b = zk.getZooKeeper().getData(path, false, null);
			String details = new String(b,"UTF-16");
			String[] spl = details.split("\n");
			for(String x: spl){
				if(x.contains("FULL")){
					isFullPrior = x.split("=")[1].trim().equals("TRUE");
				}
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isFullPrior;
	}
	
	public String getNonFullPeer(){
		for(Integer key : peerIdMap.keySet()){
			String peerPath = path+"/"+key;
			if(!isFull(peerPath)){
				return peerIdMap.get(key);
			}
		}
		return null;
	}
	
	public void updateCapacityStatus(){
		boolean isFullPrior = false;
		try {
			byte[] b = zk.getZooKeeper().getData(path, false, null);
			String details = new String(b,"UTF-16");
			String[] spl = details.split("\n");
			for(String x: spl){
				if(x.contains("FULL")){
					isFullPrior = x.split("=")[1].trim().equals("TRUE");
				}
			}
			boolean isFull = node.isFull();
			if(isFull != isFullPrior){
				String toWrite="";
				for(int i=0;i<spl.length-1;i++)
					toWrite += spl[i].trim()+"\n";
				toWrite+= "FULL=";
				if(isFull)
					toWrite += "TRUE";
				else
					toWrite += "FALSE";
				zk.getZooKeeper().setData(path, toWrite.getBytes("UTF-16"), -1);
			}
					
		} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void connectToPeer(int peerId){
		try {
			byte[]  b = zk.getZooKeeper().getData(
					ZooKeeperUtils.PATH_NODE_ROOT+"/"+groupId+"/"+peerId, false, null);
			String peerData = new String(b, "UTF-16");
			String[] lines = peerData.split("\n");
			System.out.println(peerData);
			for( String line: lines){
				if(line.contains("LISTEN=")){
					String[] connectionString = line.trim().split("=")[1].split(":");
					node.manager.connect(connectionString[0], 
							Integer.parseInt(connectionString[1]), "peer"+peerId);
					//System.out.println("PeerConnection"+connectionString[0]+":"+connectionString[1]);
					peerIdMap.put(peerId, "peer"+peerId);
					peerIds.add("peer"+peerId);
					zk.createEphemeralNode(ZooKeeperUtils.PATH_CONNECTIONS+"/"+nodeId+":"+peerId, 
							node.manager.getLocalAddress("peer"+peerId));
				}
			}
		} catch (UnsupportedEncodingException | KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void waitForConnection(int peerId){
		CountDownLatch latch = new CountDownLatch(1);
		try {
			zk.getZooKeeper().exists(ZooKeeperUtils.PATH_CONNECTIONS+"/"+peerId+":"+nodeId, 
					new ZKMonitor(zk, latch, ZKMonitor.CHILDREN_MONITOR));
			latch.await();
			byte[] b = zk.getZooKeeper().getData(ZooKeeperUtils.PATH_CONNECTIONS+"/"+peerId+":"+nodeId, 
					false, null);
			String key = new String(b,"UTF-16");
			System.out.println("peerkey:" + key);
			peerIdMap.put(peerId, key);
			peerIds.add(key);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
