package com.lib.service.main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.lib.ds.utils.MessagingUtils;
import com.lib.ds.utils.SHashMap;
import com.lib.networking.CommManager;
import com.lib.networking.ReadData;
import com.lib.networking.SocketChannelComm;
import com.lib.zk.ZookeeperHelper;

public class StorageNode {
	
	private String root="";
	CommManager manager;
	public HashMap<String,Application> applications;
	String hostname;
	int port;
	int nodeId;
	int nodeGroupId;
	int listeningPort;
	File rootDir;
	ZookeeperHelper zk;
	PeerManager peerManager;
	SHashMap<Long, PeerResponseHandler> peerHandler;
	long slaveRequestCount= 0;
	
	
	ObjectMapper mapper;
	public static final int CACHE_SIZE = 1000;
	long capacity = 1024*1024*1024*3;
	long filled = 0;
	
	CacheHelper<String, CFRecord> cache;
	
	public StorageNode(String root, int nodeId, int nodeGroupId ,String hostName, int port, int listeningPort) throws IOException{
		this.root = root;
		if(!root.endsWith("/"))
			this.root = this.root+"/";
		System.out.println(this.root);
		rootDir = new File(this.root);
		if(!rootDir.isDirectory()){
			System.out.println("StorageNode:: Building directory root structure:"+root);
			rootDir.mkdir();
			new File(this.root+"data").mkdir();
			new File(this.root+"replica").mkdir();
			new File(this.root+"replica/-1").mkdir();
			new File(this.root+"replica/-2").mkdir();
		}
		hostname = hostName;
		this.port = port;
		this.nodeId = nodeId;
		this.nodeGroupId = nodeGroupId;
		this.listeningPort = listeningPort;
		applications = new HashMap<String, Application>();
		initApplications();
		if(this.listeningPort > 0){
			manager = new CommManager(true, "0.0.0.0", listeningPort);
		} else {
			manager = new CommManager(false, "", -1);
		}
		mapper = new ObjectMapper();
		cache = new CacheHelper<String,CFRecord>(CACHE_SIZE);
		filled = FileUtils.sizeOfDirectory(new File(this.root+"data"));
		capacity = 1024L*1024L;
		capacity*= 1024*3;
		System.out.println("StorageNode:: Setting maximum capacity to "+capacity);
		System.out.println("StorageNode:: Current used up space "+filled);
	}
	
	public void initApplications(){
		File[] files =  new File(root+"data").listFiles();
		for(File f : files){
			if(f.isDirectory()){
				String appName = f.getName();
				Application app = new Application(this, f.getName() ,root, 1023);
				applications.put(appName,app);
			}
		}
	}
	
	public boolean addApplication(ObjectNode data){
		
		String appName = data.get(MessagingUtils.FIELD_APPLICATION_NAME).asText();
		System.out.println("StorageNode:: Creating Application : "+appName);
		String appDirPath[] = {root+"data/"+appName, root + "replica/-1/"+appName, 
				root + "replica/-2/"+appName};
		for(String p: appDirPath ){
			System.out.println(p);
			File appDir = new File(p);
			if(appDir.isDirectory()){
				System.out.println("StorageNode:: Error creating folder. Folder already exists : "+appDir.getAbsolutePath());
				return false;
			}
			appDir.mkdir();
		}
		try {
			for(String p: appDirPath){
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter
						(new FileOutputStream(p+"/schema.json"), StandardCharsets.UTF_8));
				writer.write(data.toString());
				writer.close();
				System.out.println("StorageNode:: Application schema written: "+ p+"/schema.json for application :"+appName );
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		Application app = new Application(this, appName, root, 1023);
		applications.put(appName, app);
		System.out.println("StorageNode:: Application successfully registered: "+appName);
		return true;
	}
	
	public void sendResponse(CFRecord result, ObjectNode request){
		//Record response = result.record;
		Application app = applications.get(
				request.get(MessagingUtils.FIELD_APPLICATION_NAME).asText());
		ObjectNode finalResponse = app.prepareCFRecord(result, request);
		boolean isSuccess = finalResponse.get(MessagingUtils.FIELD_STATUS
				).asText().equals(MessagingUtils.VALUE_SUCCESS);
		System.out.println("isSuccess"+isSuccess);
		//boolean isForwarded = request.has(MessagingUtils.FIELD_PEER_FORWARD);
		String key = request.get(MessagingUtils.FIELD_ROWKEY_LOCAL).asText();
		//update cache
		String operation = request.get(MessagingUtils.FIELD_OPERATION).asText();
		switch(operation){
		case MessagingUtils.OPERATION_GET:
		case MessagingUtils.OPERATION_SET:
			if(isSuccess)
				cache.put(key,result);
			break;
		case MessagingUtils.OPERATION_DELETE:
			if(isSuccess)
				cache.invalidate(key);
		}
		//update the filled bytes
		if(request.has(MessagingUtils.FIELD_APPENDED) && isSuccess){
			System.out.println("StorageNode:: Disk usage = "+filled );
			filled = FileUtils.sizeOfDirectory(new File(root+"data"));
			request.remove(MessagingUtils.FIELD_APPENDED);
		}
		// send final response
		//if(isSuccess || isForwarded || request.has(MessagingUtils.FIELD_FORCE_APPEND)
		//		|| !queryPeers(request)){
		
		if(finalResponse.has(MessagingUtils.FIELD_ROWKEY_LOCAL));
		finalResponse.put(MessagingUtils.FIELD_ROWKEY,key.split("#")[1]);
		finalResponse.remove(MessagingUtils.FIELD_ROWKEY_LOCAL);

		long requestId = finalResponse.get(MessagingUtils.FIELD_REQUEST_ID).asLong();
		String returnTo = request.get(MessagingUtils.FIELD_RETURN_TO).asText();
		System.out.println("StorageNode :: Sending processed response for request ID :"+requestId);
		manager.send(returnTo, finalResponse);
		//}
	}
	
	public void setupPeers(){ 
		peerManager = new PeerManager(zk, nodeGroupId, nodeId, this);
		peerHandler = new SHashMap<Long, PeerResponseHandler>();
	}
	
	public boolean queryPeers(ObjectNode request){
		System.out.println("Querying peers");
		List<String> peers = peerManager.getAllPeers();
		if(peers.size() == 0)
			return false;
		
		request.put(MessagingUtils.FIELD_PEER_FORWARD, "False");
		long requestId = request.get(MessagingUtils.FIELD_REQUEST_ID).asLong();
		String sendTo = request.get(MessagingUtils.FIELD_REQUEST_ID).asText();
		String operation = request.get(MessagingUtils.FIELD_OPERATION).asText();
		PeerResponseHandler handler = new PeerResponseHandler(requestId, peerHandler, manager, operation, 
				peers.size(), sendTo, this, peerManager, request.get(MessagingUtils.FIELD_APPLICATION_NAME).asText());
		peerHandler.put(requestId,handler);
		for(String peer: peers){
			System.out.println("Sending to peer: "+peer);
			System.out.println(manager.getRemoteAddress(peer));
			manager.send(peer, request);
		}
		
		return true;
	}
	
	public boolean removeApplication(ObjectNode node){
		boolean result = false;
		String appName = node.get(MessagingUtils.FIELD_APPLICATION_NAME).asText();
		System.out.println("StorageNode:: Removing Application : "+appName);
		String appDirPath[] = {root+"data/"+appName, root + "replica/-1/"+appName, 
				root + "replica/-2/"+appName};
		for(String p: appDirPath ){
			File appDir = new File(p);
			if(appDir.isDirectory()){
				try {
					System.out.println("StorageNode:: Deleting directory :"+appDir.getAbsolutePath());
					FileUtils.deleteDirectory(appDir);
					result = true;
				} catch (IOException e) {
					result = false;
					e.printStackTrace();
				}
			}
		}
		return result;
	}
	
	public void handleRequest(){
		System.out.println("StorageNode:: Node:"+nodeId +" NodeGroupId: "+nodeGroupId +" Ready to handle requests");
		while(true){
			ReadData r = manager.readNext(1000);
			if(r==null || r.data == null){
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			ObjectNode node = r.data;
			/*
			boolean fromPeer = node.has(MessagingUtils.FIELD_PEER_FORWARD);
			System.out.println("fromPeer:"+fromPeer);
			if(fromPeer){
				long requestId = node.get(MessagingUtils.FIELD_REQUEST_ID).asLong();
				System.out.println("peerhandler has key"+peerHandler.containsKey(requestId));
				if(peerHandler.containsKey(requestId)){
					peerHandler.get(requestId).handleResponse(r);
					continue;
				}
			}
			*/
			node.put(MessagingUtils.FIELD_RETURN_TO, r.from);
			if(!node.has(MessagingUtils.FIELD_REPLICA_INDICATOR))
				node.put(MessagingUtils.FIELD_REPLICA_INDICATOR, MessagingUtils.REPLICA_ORIGINAL);
			
			switch (node.get(MessagingUtils.FIELD_OPERATION).asText()){
			case MessagingUtils.OPERATION_REGISTER:
				System.out.println("StorageNode:: Received Application Register Request");
				if(addApplication(node))
					manager.send(r.from, getResponse(true,r.data));
				else
					manager.send(r.from, getResponse(false,r.data));
				break;
			case MessagingUtils.OPERATION_UNREGISTER:
				System.out.println("StorageNode:: Received Application Unegister Request");
				if (removeApplication(node))
					manager.send(r.from, getResponse(true,r.data));
				else
					manager.send(r.from, getResponse(false,r.data));
				break;
			case MessagingUtils.OPERATION_GET:
				System.out.println("StorageNode:: Received Get Request");
				doGet(r);
				break;
			case MessagingUtils.OPERATION_SET:
				System.out.println("StorageNode:: Received Set Request");
				doSet(r);
				break;
			case MessagingUtils.OPERATION_DELETE:System.out.println("StorageNode:: Received Get Request");
				System.out.println("StorageNode:: Received Delete Request");
				doDelete(r);
				break;
			default:
				System.out.println("StorageNode:: Unsupported operation");
			}	
		}
	}
	
	public ObjectNode getResponse(boolean success, ObjectNode request){
		ObjectNode node = mapper.createObjectNode();
		if(request.has(MessagingUtils.FIELD_REQUEST_ID))
			node.put(MessagingUtils.FIELD_REQUEST_ID, request.get(MessagingUtils.FIELD_REQUEST_ID).asText());
		if(request.has(MessagingUtils.FIELD_JOB_ID))
			node.put(MessagingUtils.FIELD_JOB_ID, request.get(MessagingUtils.FIELD_JOB_ID).asText());
		if(request.has(MessagingUtils.FIELD_CLIENT_ADDRESS))
			node.put(MessagingUtils.FIELD_CLIENT_ADDRESS, request.get(MessagingUtils.FIELD_CLIENT_ADDRESS).asText());
		if(success){
			node.put(MessagingUtils.FIELD_STATUS, "success");
			node.put(MessagingUtils.FIELD_STATUS_MESSAGE, "Operation success");
		} else {
			node.put(MessagingUtils.FIELD_STATUS, "error");
			node.put(MessagingUtils.FIELD_STATUS_MESSAGE, "Operation failed");
		}
		return node;
	}
	
	public String buildKey(ObjectNode node){
		String result = null;
		if(!node.has(MessagingUtils.FIELD_APPLICATION_NAME) ||
				!node.has(MessagingUtils.FIELD_ROWKEY) || 
				!node.has(MessagingUtils.FIELD_CF_NAME)){
			return null;
		}
		result = node.get(MessagingUtils.FIELD_APPLICATION_NAME).asText() + "#" +
				node.get(MessagingUtils.FIELD_ROWKEY).asText() + "#" +
				node.get(MessagingUtils.FIELD_CF_NAME).asText();
		node.put(MessagingUtils.FIELD_ROWKEY_LOCAL, result);
		return result;
	}
	
	public void doGet(ReadData r){
		ObjectNode node = r.data;
		String rowkey = buildKey(node);
		CFRecord cached = cache.get(rowkey);
		if(cached!=null){
			System.out.println("StorageNode:: Cache hit for get, key = "+rowkey);
			Application app = applications.get(node.get(MessagingUtils.FIELD_APPLICATION_NAME).asText());
			ObjectNode fetchedData = app.prepareCFRecord(cached, node);
			manager.send(r.from, fetchedData);
			return;
		}
		System.out.println("StorageNode:: Cache miss for get, key = "+rowkey);
		String appName = node.get(MessagingUtils.FIELD_APPLICATION_NAME).asText();
		if(applications.containsKey(appName)){
			Application app = applications.get(appName);
			app.addRequest(node);
		} else {
			manager.send(r.from, getResponse(false,r.data));
		}
	}
	
	public void doSet(ReadData r){
		ObjectNode node = r.data;
		String rowkey = buildKey(node);
		CFRecord cached = cache.get(rowkey);
		int fileNo = -1;
		if(cached!=null){
			System.out.println("StorageNode:: Cache hit for set, location of record found for update, key = "+rowkey);
			fileNo = cached.sourceFile; 
		}
		node.put(MessagingUtils.FIELD_FILE_NUMBER,fileNo);
		/*
		if(node.has(MessagingUtils.FIELD_FORCE_APPEND)){
			node.remove(MessagingUtils.FIELD_FORCE_MODIFY);
			node.put(MessagingUtils.FIELD_FORCE_APPEND,"True");
		}
		else
			node.put(MessagingUtils.FIELD_FORCE_MODIFY,"True");
		*/
		String appName = node.get(MessagingUtils.FIELD_APPLICATION_NAME).asText();
		if(applications.containsKey(appName)){
			Application app = applications.get(appName);
			app.addRequest(node);
		} else {
			manager.send(r.from, getResponse(false,r.data));
		}	
	}
	
	public void doDelete(ReadData r){
		ObjectNode node = r.data;
		String rowkey = buildKey(node);
		CFRecord cached = cache.get(rowkey);
		int fileNo = -1;
		if(cached!=null){
			System.out.println("StorageNode:: Cache hit for delete, location of record found for deletion, key = "+rowkey);
			fileNo = cached.sourceFile; 
		}
		node.put(MessagingUtils.FIELD_FILE_NUMBER,fileNo);
		String appName = node.get(MessagingUtils.FIELD_APPLICATION_NAME).asText();
		if(applications.containsKey(appName)){
			Application app = applications.get(appName);
			app.addRequest(node);
		} else {
			manager.send(r.from, getResponse(false,r.data));
		}
	}	
	
	public void connectToMaster(int listeningPort){
		String masterDetails = zk.getMasterDetails();
		String[] address = masterDetails.trim().split(":");
		System.out.println("StorageNode:: Read master details from ZooKeeper " + masterDetails );
		manager.connect(address[0], Integer.parseInt(address[1]), "master");
		String local = manager.getLocalIpAddress();
		address = local.trim().substring(1).split(":");
		String details = "LOCAL="+local;
		if(listeningPort != -1){
			details+="\nLISTEN="+address[0]+":"+listeningPort;
		}
		if(capacity - filled > 100){
			details+="\nFULL=FALSE";
		} else {
			details+="\nFULL=TRUE";
		}
		try {
			zk.createEphemeralNode("/master/"+nodeGroupId+"/"+nodeId, details);
			System.out.println("StorageNode:: Created ZooKeeper ephemeral node.");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws InterruptedException{
		if(args.length < 6){
			System.out.println("Usage: java StorageNode.java <nodeId> <nodeGroupId> <root> <masterHost> <masterPort> <listeningPort>");
			return;
		}
		
		int nodeId  = Integer.parseInt(args[0]);
		int nodeGroupId = Integer.parseInt(args[1]);
		String root = args[2];
		String masterHost = args[3];
		int masterPort = Integer.parseInt(args[4]);
		int listeningPort = Integer.parseInt(args[5]);
		
		
		try {
			StorageNode node = new StorageNode(root, nodeId, nodeGroupId, masterHost, masterPort, listeningPort);
			//node.manager.connect(masterHost, masterPort,"master");
			System.out.println("StorageNode:: Connecting to ZooKeeper");
			node.zk = new ZookeeperHelper(masterHost, masterPort);
			node.zk.connect();
			node.connectToMaster(listeningPort);
			if(listeningPort != -1){
				new Thread(node.manager).start();
			}
			node.setupPeers();
			node.handleRequest();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean isFull() {
		return capacity - filled < 100;
	}
	
}
