package com.master.responseHandler;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.node.ObjectNode;

import com.lib.ds.utils.MessagingUtils;
import com.lib.networking.ReadData;
import com.master.beans.ResponseBean;
import com.master.service.MasterService;

public class ResponseHandler implements Runnable
{
	public static Map<Long, ResponseBean> responses = new HashMap<Long, ResponseBean>();
	MasterService master;
	
	public ResponseHandler(MasterService master)
	{
		this.master = master;
		new Thread(this).start(); 
	}
	
	@Override
	public void run() 
	{
		processResponse();
	}

	private void processResponse() 
	{
		System.out.println("Checking process response");
		while(true)
		{
			ReadData r = master.getNodeManager().readNext(1000);
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
			if(node.has(MessagingUtils.FIELD_REQUEST_ID) && responses.containsKey(node.get(MessagingUtils.FIELD_REQUEST_ID).asLong()))
			{
				System.out.println("adding response");
				responses.get(node.get(MessagingUtils.FIELD_REQUEST_ID).asLong()).addResponses(node);
				if(responses.get(node.get(MessagingUtils.FIELD_REQUEST_ID).asLong()).getJobTracker() == 0)
				{
					responses.get(node.get(MessagingUtils.FIELD_REQUEST_ID).asLong()).mergeResponses();
				}
			}
		}
	}	
}