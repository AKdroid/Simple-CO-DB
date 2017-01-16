package com.master.service;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.lib.zk.ZKMonitor;
import com.lib.zk.ZooKeeperUtils;
import com.lib.zk.ZookeeperHelper;

public class ClusterMonitor implements Runnable
{
	private HashSet<String> clusterNodes;
	private ZookeeperHelper zk;
	private int clusterId;
	private String groupPath;
	private HashMap<Integer,String> clusterNodeIdMap;
    private MasterService masterService;
	
	public ClusterMonitor(int clusterId, ZookeeperHelper zk, MasterService masterService)
	{;
		this.masterService = masterService;
		clusterNodes = new HashSet<String>();
		clusterNodeIdMap = new HashMap<Integer,String>();
		this.clusterId = clusterId;
		this.zk = zk;
		this.groupPath = ZooKeeperUtils.PATH_NODE_ROOT+"/"+this.clusterId;
		this.updateChildren();
		new Thread(this).start();
	}
	
	public synchronized void updateChildren()
	{
		try 
		{
			List<String> children = zk.getZooKeeper().getChildren(groupPath, false);
			Set<Integer> childNodes = new HashSet<Integer>(clusterNodeIdMap.keySet());
			for(String child: children)
			{
				String childPath = groupPath+"/"+child;
				if( !childNodes.contains(Integer.parseInt(child)) )
				{
					byte[] b = zk.getZooKeeper().getData(childPath, false, null);
					String data = new String(b,"UTF-16");
					String[] lines = data.split("\n");
					for(String line: lines)
					{
						if(line.contains("LOCAL"))
						{
							clusterNodes.add(line.split("=")[1].trim());
							System.out.println("Added new node in cluster: "+clusterId +" child: "+child);
							clusterNodeIdMap.put(Integer.parseInt(child), line.split("=")[1].trim());
							masterService.addSchemas(clusterNodeIdMap.get(Integer.parseInt(child)));
						}
					}
				} 
				else 
				{
					childNodes.remove(Integer.parseInt(child));
				}
			}
			
			for(Integer node : childNodes)
			{
				String x = clusterNodeIdMap.get(node);
				clusterNodes.remove(x);
				clusterNodeIdMap.remove(node);
				System.out.println("Node disconnected: "+node);
			}
			
		} 
		catch (KeeperException | InterruptedException | UnsupportedEncodingException e) 
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() 
	{
		System.out.println("Tracking node changes in cluster"+clusterId);
		
		while(true)
		{
			CountDownLatch latch = new CountDownLatch(1);
			try 
			{
				zk.getZooKeeper().getChildren(groupPath, 
						new ZKMonitor(zk,latch,ZKMonitor.CHILDREN_MONITOR));
				latch.await();
				updateChildren();
			} 
			catch (KeeperException | InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
		
	}
	
	public synchronized String getRandomNode()
	{
		Random random = new Random();
		int choice = random.nextInt(clusterNodes.size());
		for(String val : clusterNodes)
		{
			choice--;
			if(choice == -1)
				return val;
		}
	    return "";	
	}
	
	public synchronized String getNodeString(int nodeId)
	{
		if(clusterNodeIdMap.containsKey(nodeId))
			return clusterNodeIdMap.get(nodeId);
		return null;
	}	
}