package com.lib.service.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.lib.ds.utils.HashUtil;
import com.lib.ds.utils.MessagingUtils;
import com.lib.ds.utils.PCQueue;

public class Application implements Runnable{

	String name;
	String root;
	String dataFolder;
	String replicaFolder1,replicaFolder2;
	HashMap<String,CFMetadataManager> columnFamilies;
	HashMap<String,CFMetadataManager> columnFamiliesR1;
	HashMap<String,CFMetadataManager> columnFamiliesR2;
	//HashMap<String,CFMetadataManager> columnFamiliesReplicas;
	int filegroupSize;
	PCQueue<ObjectNode> requestQueue;
	public static final int QUEUE_SIZE = 50000;
	boolean handleRequestFlag = true;
	ObjectMapper mapper;
	StorageNode parentNode;
	
	public Application(StorageNode parent, String appName, String rt, int filegroupSize){
		this.replicaFolder1 = this.root+"replica/-1/"+this.name;
		this.name = appName;
		if(!(rt.charAt(rt.length()-1)=='/'))
			root = rt + "/";
		else
			root = rt;
		this.filegroupSize = filegroupSize;
		columnFamilies = new HashMap<String,CFMetadataManager>();
		columnFamiliesR1 = new HashMap<String,CFMetadataManager>(); //replica 1
		columnFamiliesR2 = new HashMap<String,CFMetadataManager>(); //replica 2
		
		dataFolder = root + "data/";
		this.dataFolder = this.root+"data/"+this.name;
		this.replicaFolder1 = this.root+"replica/-1/"+this.name;
		this.replicaFolder2 = this.root+"replica/-2/"+this.name;
		requestQueue = new PCQueue<ObjectNode>(QUEUE_SIZE);
		handleRequestFlag = true;
		mapper = new ObjectMapper();
		this.parentNode = parent;
		readSchema();
		new Thread(this).start();
	}
	
	public void readSchema(){
		
		String schemaFilePath = root+"data/"+name+"/schema.json";
		System.out.println("Application:: Initializing application data from schema:"+schemaFilePath);
		try {
			JsonNode node = (ObjectNode) new ObjectMapper().readTree(new File(schemaFilePath));
			JsonNode columnFamily = node.get(MessagingUtils.FIELD_CF_LIST);
			Iterator<String> cfnames = columnFamily.getFieldNames();
			while(cfnames.hasNext()){
				String dirName = cfnames.next();
				File appDir = new File(dataFolder + "/" + dirName);
				if(!appDir.isDirectory()){
					appDir.mkdir();
					new File(replicaFolder1+"/"+dirName).mkdir();
					System.out.println(replicaFolder1+"///"+dirName);
					new File(replicaFolder2+"/"+dirName).mkdir();
					System.out.println(replicaFolder1+"///"+dirName);
				}
				JsonNode cf = columnFamily.get(dirName);
				System.out.println("Application:: Detected column family :"+ dirName);
				ArrayList<String> columnNames = new ArrayList<String>(10);
				for(JsonNode col : cf.get(MessagingUtils.FIELD_COLUMN_NAMES)){
					System.out.println("Application:: Detected column "+ col.asText() +" in column family :"+ dirName);
					columnNames.add(col.asText());
				}
				
				columnFamilies.put(dirName, new CFMetadataManager(
					this.name, dirName, dataFolder+"/"+dirName,columnNames.toArray(new String[columnNames.size()]),this.filegroupSize
				));	
				columnFamiliesR1.put(dirName, new CFMetadataManager(
					this.name, dirName, replicaFolder1+"/"+dirName,columnNames.toArray(new String[columnNames.size()]),this.filegroupSize
				));
				columnFamiliesR2.put(dirName, new CFMetadataManager(
					this.name, dirName, replicaFolder2+"/"+dirName,columnNames.toArray(new String[columnNames.size()]),this.filegroupSize
				));
			
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	} 
	
	public void addRequest(ObjectNode request){
		System.out.println("Application:: Adding to the request queue for processing : "
				+ request.get(MessagingUtils.FIELD_REQUEST_ID).asText());
		requestQueue.add(request);
	}
	
	public void handleRequests(){
		System.out.println("Application: Ready to process requests for application"+name);
		while(handleRequestFlag){		
			ObjectNode request = requestQueue.poll();
			CFRecord record;
			if(request != null){
				System.out.println("Application: Processing request for application"+name);
				record = handleRequest(request);
				parentNode.sendResponse(record, request);
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
	}
	
	public CFRecord handleRequest(ObjectNode request){
		CFRecord record = null;
		if(!request.has(MessagingUtils.FIELD_OPERATION)){
			System.out.println("Application:: Request operation code not present. Aborting operation");
			return null;
		}
		String cfname= null;
		String rowKey;
		int groupNum;
		int fileNum = -1;
		if(!request.has(MessagingUtils.FIELD_CF_NAME)){
			System.out.println("Application:: Request Column Family name not present. Aborting operation");
			return null;
		} else {
			cfname = request.get(MessagingUtils.FIELD_CF_NAME).asText();
		}
		if(!request.has(MessagingUtils.FIELD_REPLICA_INDICATOR))
			request.put(MessagingUtils.FIELD_REPLICA_INDICATOR, MessagingUtils.REPLICA_ORIGINAL);
		String operateOn = request.get(MessagingUtils.FIELD_REPLICA_INDICATOR).asText();
		HashMap<String, CFMetadataManager> mgr = null;
		
		switch(operateOn){
		case MessagingUtils.REPLICA_ORIGINAL:
			System.out.println("Application:: Processing original data");
			mgr = columnFamilies;
			break;
		case MessagingUtils.REPLICA_PREV:
			System.out.println("Application:: Processing replica data (-1)");
			mgr = columnFamiliesR1;
			break;
		case MessagingUtils.REPLICA_PREV_PREV:
			System.out.println("Application:: Processing replica data (-2)");
			mgr = columnFamiliesR2;
			break;
		default:
			System.out.println("Application:: Invalid Replica indicator");
			return null;
		}
		
		switch(request.get(MessagingUtils.FIELD_OPERATION).asText()){
		case MessagingUtils.OPERATION_GET:
			rowKey = request.get(MessagingUtils.FIELD_ROWKEY_LOCAL).asText();
			groupNum = HashUtil.getHash(rowKey,1,filegroupSize);
			System.out.println("Application:: Processing GET request RowKey = " + rowKey + "Filegroup:" + groupNum);
			record = mgr.get(cfname).get(rowKey, groupNum);
			break;
		case MessagingUtils.OPERATION_SET:
			rowKey = request.get(MessagingUtils.FIELD_ROWKEY_LOCAL).asText();
			groupNum = HashUtil.getHash(rowKey,1,filegroupSize);
			fileNum = request.get(MessagingUtils.FIELD_FILE_NUMBER).asInt(); 
			HashMap<String,String> map = new HashMap<String,String>();
			for(String column : mgr.get(cfname).columns){
				if(request.has(column)){
					map.put(column, request.get(column).asText());
				}
			}
			/*
			if(request.has(MessagingUtils.FIELD_FORCE_MODIFY))
				record = mgr.get(cfname).set(rowKey, groupNum, map, fileNum,true,false);
			else if(request.has(MessagingUtils.FIELD_FORCE_APPEND))
				record = mgr.get(cfname).set(rowKey, groupNum, map, fileNum,false,true);
			else
			*/
			System.out.println("Application:: Processing SET request RowKey = " + rowKey + "Filegroup:" + groupNum);
			record = mgr.get(cfname).set(rowKey, groupNum, map, fileNum, false, false);
			break;
		case MessagingUtils.OPERATION_DELETE:
			rowKey = request.get(MessagingUtils.FIELD_ROWKEY_LOCAL).asText();
			groupNum = HashUtil.getHash(rowKey,1,filegroupSize);
			fileNum = request.get(MessagingUtils.FIELD_FILE_NUMBER).asInt(); 
			System.out.println("Application:: Processing DELETE request RowKey = " + rowKey + "Filegroup:" + groupNum);
			record = mgr.get(cfname).delete(rowKey, groupNum, fileNum);
			break;
		default:
			System.out.println("Invalid request operation code");
		}
		return record;
	}
	
	public void stopHandlingRequests(){
		handleRequestFlag = false;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		handleRequests();
	}
	
	public ObjectNode prepareCFRecord(CFRecord record, ObjectNode request){
		ObjectNode node = mapper.createObjectNode();
		String[] copyFields = {
				MessagingUtils.FIELD_REQUEST_ID,
				MessagingUtils.FIELD_JOB_ID,
				MessagingUtils.FIELD_PEER_FORWARD,
				MessagingUtils.FIELD_CLIENT_ADDRESS,
				MessagingUtils.FIELD_FORCE_APPEND,
				MessagingUtils.FIELD_FORCE_MODIFY
		};
		
		for(String field: copyFields){
			if(request.has(field))
				node.put(field, request.get(field).asText());
		}
		
		node.put(MessagingUtils.FIELD_APPLICATION_NAME,name);
		if(record != null){
			node.put(MessagingUtils.FIELD_CF_NAME, record.CFName);
			if(record.record != null){
			node.put(MessagingUtils.FIELD_ROWKEY ,record.record.getData("RowKey"));
			node.put(MessagingUtils.FIELD_ROWKEY_LOCAL ,record.record.getData(MessagingUtils.FIELD_ROWKEY_LOCAL));
			if(record.isAppend)
				node.put(MessagingUtils.FIELD_APPENDED, "True");
			ObjectNode cfNode = mapper.createObjectNode();
			HashMap<String,String> recordMap = record.record.getMap();
				for(String key: recordMap.keySet()){
					if(!key.equals("Active") && !(key.equals(MessagingUtils.FIELD_ROWKEY_LOCAL))){
						if(recordMap.get(key)!= null)
							cfNode.put(key, recordMap.get(key));
						else
							cfNode.put(key, "");
					}
				}
				node.put(record.CFName, cfNode);
			}
			node.put(MessagingUtils.FIELD_STATUS, MessagingUtils.VALUE_SUCCESS);
			node.put(MessagingUtils.FIELD_STATUS_MESSAGE, "Operation success");
			
		} else {
			node.put(MessagingUtils.FIELD_STATUS, MessagingUtils.VALUE_ERROR);
			node.put(MessagingUtils.FIELD_STATUS_MESSAGE, "Operation failed");
		}
		
		return node;
	}
}
