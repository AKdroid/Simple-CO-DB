package com.lib.service.main;

import org.codehaus.jackson.node.ObjectNode;

import com.lib.ds.utils.MessagingUtils;
import com.lib.ds.utils.SHashMap;
import com.lib.networking.CommManager;
import com.lib.networking.ReadData;

public class PeerResponseHandler {

	String operation;
	SHashMap<Long, PeerResponseHandler> store;
	CommManager mgr;
	int count;
	long requestId;
	String sendTo; 
	ObjectNode request;
	PeerManager peerManager;
	StorageNode service;
	String app;

	public PeerResponseHandler(long requestId, SHashMap<Long, PeerResponseHandler> x,
			CommManager mgr, String opType, int count, String sendTo, StorageNode node, PeerManager m, String appName){
		System.out.println("Created peer response handler");
		this.requestId = requestId;
		store = x;
		this.mgr = mgr;
		this.sendTo = sendTo;
		this.operation = opType;
		this.count = count;
		service = node;
		this.peerManager = m;
		app =appName;
	}

	public void handleResponse(ReadData r){
		System.out.println("Called handler peer response");
		ObjectNode node = r.data;
		count--;
		if(node.get(MessagingUtils.FIELD_STATUS).asText().equals(MessagingUtils.VALUE_SUCCESS)){
			node.remove(MessagingUtils.FIELD_RETURN_TO);
			node.remove(MessagingUtils.FIELD_PEER_FORWARD);
			mgr.send(sendTo, node);
			store.remove(requestId);
			return;
		}
		if(count == 0){
			System.out.println("count is 0");
			System.out.println(node.toString());
			if(operation.equals(MessagingUtils.OPERATION_SET) && node.has(MessagingUtils.FIELD_FORCE_MODIFY)){
				System.out.println("operation set and operation append");
				node.remove(MessagingUtils.FIELD_FORCE_MODIFY);
				node.put(MessagingUtils.FIELD_FORCE_APPEND, "True");
				if(service.isFull()){
					String address = peerManager.getNonFullPeer();
					if(address != null)
						mgr.send(address, node);
					else{
						node.put(MessagingUtils.FIELD_RETURN_TO, sendTo);
						service.applications.get(node.get(app)).addRequest(node);
					} 
				} else {
					node.put(MessagingUtils.FIELD_RETURN_TO, sendTo);
					service.applications.get(node.get(MessagingUtils.FIELD_APPLICATION_NAME)).addRequest(node);
				}

			} else {
				node.remove(MessagingUtils.FIELD_RETURN_TO);
				node.remove(MessagingUtils.FIELD_PEER_FORWARD);
				node.remove(MessagingUtils.FIELD_FORCE_APPEND);
				store.remove(requestId);
				mgr.send(sendTo, node);
			}

		}

	}

}
