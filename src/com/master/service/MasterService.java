package com.master.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.codehaus.jackson.node.ObjectNode;

import com.lib.ds.utils.Configurations;
import com.lib.ds.utils.MessagingUtils;
import com.lib.networking.CommManager;
import com.lib.networking.ReadData;
import com.lib.zk.ZookeeperHelper;
import com.master.requestHandler.RequestHandler;
import com.master.responseHandler.ResponseHandler;

public class MasterService
{
	private CommManager clientManager;
	private CommManager nodeManager;

	private RequestHandler requestHandler;
	public int numOfClusters = 0;
	
	ZookeeperHelper zkhelper;
	
	ArrayList<ClusterMonitor> clusters; 
	
	public MasterService() throws Exception
	{
		new Configurations();
		clientManager = new CommManager(true, "0.0.0.0", Configurations.CLIENT_LISTEN_PORT);
		nodeManager = new CommManager(true, "0.0.0.0", Configurations.INTERNAL_LISTEN_PORT);
		zkhelper = new ZookeeperHelper(Configurations.ZOOKEEPER_HOST, Configurations.ZOOKEEPER_PORT);
		new ResponseHandler(this);
		requestHandler  = new RequestHandler(this);
		numOfClusters = Configurations.NUMBER_OF_CLUSTERS;
		clusters = new ArrayList<ClusterMonitor>(Configurations.NUMBER_OF_CLUSTERS);
	}
	
	public CommManager getNodeManager() 
	{
		return nodeManager;
	}
	
	public CommManager getClientManager() 
	{
		return clientManager;
	}
	
	public void setupZookeeper() throws IOException, InterruptedException
	{
		int i;
		zkhelper.connect();
		try 
		{
			zkhelper.initTree(Configurations.NUMBER_OF_CLUSTERS);
			for(i=0;i<Configurations.NUMBER_OF_CLUSTERS;i++)
			{
				clusters.add(new ClusterMonitor(i, zkhelper, this));
			}
			zkhelper.addMasterDetails(Configurations.CONNECTION_STRING);
		} 
		catch (KeeperException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void handleRequest()
	{
		while(true)
		{
			ReadData r = clientManager.readNext(1000);
			if(r == null || r.data == null)
			{
				try 
				{
					Thread.sleep(100);
				} 
				catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
				continue;
			}
			ObjectNode node = r.data;
			node.put(MessagingUtils.FIELD_CLIENT_ADDRESS, r.from);
			System.out.println("adding to queue : " + node.toString());
			requestHandler.addToQueue(node);
		}
	}
	
	public static void main(String[] args)
	{	
		try
		{
			MasterService node = new MasterService();
			new Thread(node.clientManager).start();
			new Thread(node.nodeManager).start();
			node.setupZookeeper();
			node.requestHandler.setClusters(node.clusters);
			node.handleRequest();		
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public List<ClusterMonitor> getClusters() 
	{
		return clusters;
	}

	public void addSchemas(String node) 
	{
		requestHandler.addSchemas(node);
	}
}