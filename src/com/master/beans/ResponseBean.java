package com.master.beans;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.lib.ds.utils.MessagingUtils;
import com.master.service.MasterService;
import com.lib.networking.CommManager;


public class ResponseBean 
{
	private int jobTracker;
	private ObjectNode[] responses;
	private int index = 0;
	private String operation;
	private ObjectMapper mapper = new ObjectMapper();
	MasterService master;
	Map<Long, ResponseBean> store;
	long requestId;
	
	public ResponseBean(int size, String operation, MasterService master, 
			Map<Long, ResponseBean> store, long requestId)
	{
		setJobTracker(size);
		responses = new ObjectNode[size];
		this.operation = operation;
		this.master = master;
		this.store = store;
		this.requestId = requestId;
	}
	
	public void addResponses(ObjectNode node)
	{
		responses[index] = node;
		index++;
		setJobTracker(getJobTracker() - 1);
	}
	
	public void mergeResponses()
	{
		System.out.println("merging response"+this.operation);
		switch(this.operation)
		{
			case MessagingUtils.OPERATION_REGISTER:
			case MessagingUtils.OPERATION_UNREGISTER:
			case MessagingUtils.OPERATION_SET:
			case MessagingUtils.OPERATION_DELETE:
				mergeRegisterResponses();
				break;
			case MessagingUtils.OPERATION_GET:
				mergeGetResponses();
				break;
		}
	}

	private void mergeRegisterResponses() 
	{
		for(int i=0;i<responses.length;i++){
			if(responses[i].get(MessagingUtils.FIELD_STATUS).asText().equals(MessagingUtils.ERROR))
			{
				responses[i].remove(MessagingUtils.FIELD_REQUEST_ID);
				responses[i].remove(MessagingUtils.FIELD_JOB_ID);
				String clientAdd = responses[i].get(MessagingUtils.FIELD_CLIENT_ADDRESS).asText();
				responses[i].remove(MessagingUtils.FIELD_CLIENT_ADDRESS);
				master.getClientManager().send(clientAdd, responses[i]);
				store.remove(requestId);
				return;
			}
		}
		responses[0].remove(MessagingUtils.FIELD_REQUEST_ID);
		responses[0].remove(MessagingUtils.FIELD_JOB_ID);
		String clientAdd = responses[0].get(MessagingUtils.FIELD_CLIENT_ADDRESS).asText();
		responses[0].remove(MessagingUtils.FIELD_CLIENT_ADDRESS);
		store.remove(requestId);
		master.getClientManager().send(clientAdd, responses[0]);
	}

	private void mergeGetResponses() 
	{
		for(int i=0;i<responses.length;i++)
		{
			if(responses[i].get(MessagingUtils.FIELD_STATUS).asText().equals(MessagingUtils.ERROR))
			{
				responses[i].remove(MessagingUtils.FIELD_REQUEST_ID);
				responses[i].remove(MessagingUtils.FIELD_JOB_ID);
				String clientAdd = responses[i].get(MessagingUtils.FIELD_CLIENT_ADDRESS).asText();
				responses[i].remove(MessagingUtils.FIELD_CLIENT_ADDRESS);
				master.getClientManager().send(clientAdd, responses[i]);
				store.remove(requestId);
				return;
			}
		}
		ObjectNode jsonResponse = mapper.createObjectNode();
		jsonResponse.put(MessagingUtils.FIELD_APPLICATION_NAME, responses[0].get(MessagingUtils.FIELD_APPLICATION_NAME));
		jsonResponse.put(MessagingUtils.FIELD_ROWKEY, responses[0].get(MessagingUtils.FIELD_ROWKEY));
		jsonResponse.put(MessagingUtils.FIELD_STATUS, responses[0].get(MessagingUtils.FIELD_STATUS));
		jsonResponse.put(MessagingUtils.FIELD_STATUS_MESSAGE, responses[0].get(MessagingUtils.FIELD_STATUS_MESSAGE));
		ObjectNode columnFamilies = mapper.createObjectNode();
		for(int i=0;i<responses.length;i++)
		{
			String columnFamily = responses[i].get(MessagingUtils.FIELD_CF_NAME).asText();
			columnFamilies.put(columnFamily, responses[i].get(columnFamily));
		}
		jsonResponse.put(MessagingUtils.FIELD_CF_LIST, columnFamilies);
		//check for existence and send back only if it exists
		String clientAdd = responses[0].get(MessagingUtils.FIELD_CLIENT_ADDRESS).asText();
		store.remove(requestId);
		master.getClientManager().send(clientAdd, jsonResponse);
	}
	
	public int getJobTracker() {
		return jobTracker;
	}

	public void setJobTracker(int jobTracker) {
		this.jobTracker = jobTracker;
	}
}