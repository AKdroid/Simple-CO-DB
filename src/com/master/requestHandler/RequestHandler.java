package com.master.requestHandler;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import com.lib.ds.utils.Configurations;
import com.lib.ds.utils.HashUtil;
import com.lib.ds.utils.MessagingUtils;
import com.lib.ds.utils.PCQueue;
import com.master.beans.ApplicationBean;
import com.master.beans.ResponseBean;
import com.master.responseHandler.ResponseHandler;
import com.master.service.ClusterMonitor;
import com.master.service.MasterService;

public class RequestHandler implements Runnable
{
	private ObjectMapper mapper = new ObjectMapper();
	private static long requestID = 0;

	private static PCQueue<ObjectNode> requestQueue = new PCQueue<ObjectNode>(50000);
	MasterService master;
	List<ClusterMonitor> clusters;
	
	public RequestHandler(MasterService master) throws Exception
	{
		this.master = master;
		clusters = master.getClusters();
		new Thread(this).start(); 
		
	}
	
	public void setClusters(List<ClusterMonitor> lst){
		clusters = lst;
	}

	public void addToQueue(ObjectNode requestNode)
	{
		requestQueue.add(requestNode);
	}
	
	public void requestHandler(ObjectNode jsonRequest)
	{
		ObjectNode response = mapper.createObjectNode();
		try
		{
			if(master.getNodeManager().getClientSockets().keySet().size() != 0)
			{
				if(jsonRequest != null && jsonRequest.has(MessagingUtils.FIELD_OPERATION))
				{
					ApplicationBean appBean;
					String requestType = jsonRequest.get(MessagingUtils.FIELD_OPERATION).asText();
					switch (requestType) 
					{
						case MessagingUtils.OPERATION_REGISTER:
							appBean = parseRegisterJson(jsonRequest);
							schemaRegistering(jsonRequest, appBean);
							break;
						case MessagingUtils.OPERATION_UNREGISTER:
							if(jsonRequest.has(MessagingUtils.FIELD_APPLICATION_NAME) && !jsonRequest.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim().equalsIgnoreCase(""))
							{
								schemaUnregistering(jsonRequest);
							}
							else
							{
								throw new Exception("Application name missing.");
							}
							break;
						case MessagingUtils.OPERATION_DELETE:
							if(jsonRequest.has(MessagingUtils.FIELD_APPLICATION_NAME) && !jsonRequest.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim().equalsIgnoreCase("")
									&& jsonRequest.has(MessagingUtils.FIELD_ROWKEY) && !jsonRequest.get(MessagingUtils.FIELD_ROWKEY).asText().trim().equalsIgnoreCase(""))
							{
								String appName = jsonRequest.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim();
								String rowKey = jsonRequest.get(MessagingUtils.FIELD_ROWKEY).asText().trim();
								deleteData(jsonRequest, appName, rowKey);
							}
							else
							{
								throw new Exception("Application name or Row Key missing.");
							}
							break;
						case MessagingUtils.OPERATION_GET:
							appBean = parseGetJson(jsonRequest);
							getData(jsonRequest, appBean);
							break;
						case MessagingUtils.OPERATION_SET:
							appBean = parseSetJson(jsonRequest);
							setData(jsonRequest, appBean);
							break;
						default:
							throw new Exception("Invalid Operation.");
					}
				}
				else
				{
					throw new Exception("Operation does not exist.");
				}
			}
			else
			{				
				throw new Exception("Oops..!!! Something went wrong. Please try again later.");
			}
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
			response.put(MessagingUtils.ERROR, 1);
			response.put("message", e.getMessage());
			master.getClientManager().send(jsonRequest.get(MessagingUtils.FIELD_CLIENT_ADDRESS).asText(), response);
		}
	}

	private void getData(ObjectNode jsonRequest, ApplicationBean appBean)
	{
		try 
		{
			Map<String,List<String>> colFams = appBean.getColumnFamilies();
			requestID++;
			Map<String, Object> appJson = new HashMap<String, Object>();
			appJson.put(MessagingUtils.FIELD_APPLICATION_NAME, appBean.getAppName());
			appJson.put(MessagingUtils.FIELD_ROWKEY, appBean.getRowKey());
			appJson.put(MessagingUtils.FIELD_REQUEST_ID, requestID);
			appJson.put(MessagingUtils.FIELD_OPERATION, MessagingUtils.OPERATION_GET);
			appJson.put(MessagingUtils.FIELD_CLIENT_ADDRESS, jsonRequest.get(MessagingUtils.FIELD_CLIENT_ADDRESS));
			int count = 1;
			for(String colFam:colFams.keySet())
			{
				int groupId =  findClusters(appBean.getAppName()+colFam+appBean.getRowKey()
														,master.numOfClusters);
				appJson.put(MessagingUtils.FIELD_CF_NAME, colFam);
				appJson.put(MessagingUtils.FIELD_JOB_ID, count);
				ObjectNode jsonNode = (ObjectNode) mapper.readTree(mapper.writeValueAsString(appJson));
				
				String nodeAddress = clusters.get(groupId).getRandomNode();
				if(nodeAddress != null)
					master.getNodeManager().send(nodeAddress, jsonNode);
				else
					System.out.println("Cannot send to the groupId. No slave node available");
				count++;
			}
			ResponseHandler.responses.put(requestID, 
					new ResponseBean(count-1, MessagingUtils.OPERATION_GET, master, 
							ResponseHandler.responses ,requestID));
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	private void setData(ObjectNode jsonRequest, ApplicationBean appBean) throws Exception
	{
		try 
		{
			Map<String,Map<String, String>> colFams = appBean.getColumns();
			requestID++;
			int count = 1;
			for(String colFam:colFams.keySet())
			{
				Map<String, Object> appJson = new HashMap<String, Object>();
				appJson.put(MessagingUtils.FIELD_APPLICATION_NAME, appBean.getAppName());
				appJson.put(MessagingUtils.FIELD_ROWKEY, appBean.getRowKey());
				appJson.put(MessagingUtils.FIELD_REQUEST_ID, requestID);
				appJson.put(MessagingUtils.FIELD_OPERATION, MessagingUtils.OPERATION_SET);
				appJson.put(MessagingUtils.FIELD_CLIENT_ADDRESS, jsonRequest.get(MessagingUtils.FIELD_CLIENT_ADDRESS));
				
				int groupId =  findClusters(appBean.getAppName()+colFam+appBean.getRowKey(), master.numOfClusters);
				appJson.put(MessagingUtils.FIELD_CF_NAME, colFam);
				Map<String,String> colVal = appBean.getColumns().get(colFam);
				for(Map.Entry<String,String> e: colVal.entrySet())
				{
					appJson.put(e.getKey(), e.getValue());
				}
				ObjectNode jsonNode = (ObjectNode) mapper.readTree(mapper.writeValueAsString(appJson));
				String nodeAddressClusOne = clusters.get((groupId)%Configurations.NUMBER_OF_CLUSTERS).getRandomNode();
				String nodeAddressClusTwo = clusters.get((groupId+1)%Configurations.NUMBER_OF_CLUSTERS).getRandomNode();
				String nodeAddressClusThree = clusters.get((groupId+2)%Configurations.NUMBER_OF_CLUSTERS).getRandomNode();
				if(nodeAddressClusOne != null && nodeAddressClusTwo != null && nodeAddressClusThree != null)
				{
					jsonNode.put(MessagingUtils.FIELD_JOB_ID, count);
					jsonNode.put(MessagingUtils.REPLICA_ID, 0);
					master.getNodeManager().send(nodeAddressClusOne, jsonNode);
					jsonNode.put(MessagingUtils.FIELD_JOB_ID, ++count);
					jsonNode.put(MessagingUtils.REPLICA_ID, -1);
					master.getNodeManager().send(nodeAddressClusTwo, jsonNode);
					jsonNode.put(MessagingUtils.FIELD_JOB_ID, ++count);
					jsonNode.put(MessagingUtils.REPLICA_ID, -2);
					master.getNodeManager().send(nodeAddressClusThree, jsonNode);
				}
				else
					System.out.println("Cannot send to the groupId. No slave node available");
				count++;
			}
			ResponseHandler.responses.put(requestID, 
					new ResponseBean(count-1, MessagingUtils.OPERATION_SET, master, ResponseHandler.responses ,requestID));
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			throw e;
		}
	}

	private void deleteData(ObjectNode jsonRequest, String appname, String row_key) throws Exception 
	{
		try 
		{
			JsonNode jsonSchema = mapper.readTree(new File(Configurations.ROOT_DIRECTORY+appname+"/"+appname+".json"));
			requestID++;
			Map<String, Object> appJson = new HashMap<String, Object>();
			appJson.put(MessagingUtils.FIELD_APPLICATION_NAME, appname);
			appJson.put(MessagingUtils.FIELD_ROWKEY, row_key);
			appJson.put(MessagingUtils.FIELD_REQUEST_ID, requestID);
			appJson.put(MessagingUtils.FIELD_OPERATION, MessagingUtils.OPERATION_DELETE);
			appJson.put(MessagingUtils.FIELD_CLIENT_ADDRESS, jsonRequest.get(MessagingUtils.FIELD_CLIENT_ADDRESS));
			int count = 1;
			JsonNode cfs = jsonSchema.get(MessagingUtils.FIELD_CF_LIST);
			Iterator<Entry<String,JsonNode>> nodes = cfs.getFields();
			while(nodes.hasNext())
			{
				Entry<String, JsonNode> node = nodes.next();
				String colFam = node.getKey();
				int groupId = findClusters(appname+colFam+row_key, master.numOfClusters);
				appJson.put(MessagingUtils.FIELD_CF_NAME, colFam);
				ObjectNode jsonNode = (ObjectNode) mapper.readTree(mapper.writeValueAsString(appJson));
				String nodeAddressClusOne = clusters.get((groupId)%Configurations.NUMBER_OF_CLUSTERS).getRandomNode();
				String nodeAddressClusTwo = clusters.get((groupId+1)%Configurations.NUMBER_OF_CLUSTERS).getRandomNode();
				String nodeAddressClusThree = clusters.get((groupId+2)%Configurations.NUMBER_OF_CLUSTERS).getRandomNode();
				if(nodeAddressClusOne != null && nodeAddressClusTwo != null && nodeAddressClusThree != null)
				{
					jsonNode.put(MessagingUtils.FIELD_JOB_ID, count);
					jsonNode.put(MessagingUtils.REPLICA_ID, 0);
					master.getNodeManager().send(nodeAddressClusOne, jsonNode);
					jsonNode.put(MessagingUtils.FIELD_JOB_ID, ++count);
					jsonNode.put(MessagingUtils.REPLICA_ID, -1);
					master.getNodeManager().send(nodeAddressClusTwo, jsonNode);
					jsonNode.put(MessagingUtils.FIELD_JOB_ID, ++count);
					jsonNode.put(MessagingUtils.REPLICA_ID, -2);
					master.getNodeManager().send(nodeAddressClusThree, jsonNode);
				}
				else
					System.out.println("Cannot send to the groupId. No slave node available");
				count++;
			}
			ResponseHandler.responses.put(requestID,
					new ResponseBean(count-1, MessagingUtils.OPERATION_DELETE, master, ResponseHandler.responses ,requestID));
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			throw e;
		}
	}

	private int findClusters(String key, int numClusters) 
	{
		int key1 = HashUtil.getHash(key, 1, numClusters);
		//int key2 = HashUtil.getHash(key, 2, numClusters-1);
		//int key3 = HashUtil.getHash(key, 3, numClusters-2);
		//int[] hashes = {key1,key2,key3};
		//int[] clusters = HashUtil.sampleWithoutReplacement(numClusters, hashes);
		return key1;
	}

	private ApplicationBean parseRegisterJson(ObjectNode jsonNode) throws Exception 
	{
		ApplicationBean appBean;
		try
		{
			if(jsonNode.has(MessagingUtils.FIELD_APPLICATION_NAME) && !jsonNode.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim().equalsIgnoreCase("")
					&& jsonNode.has(MessagingUtils.FIELD_CF_LIST))
			{
				appBean = new ApplicationBean(jsonNode.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim());
				int columnFamilyCount = 0;
				JsonNode cfs = jsonNode.get(MessagingUtils.FIELD_CF_LIST);
				Iterator<Entry<String,JsonNode>> nodes = cfs.getFields();
				while(nodes.hasNext())
				{
					Entry<String, JsonNode> node = nodes.next();
					String columnFamilyName = node.getKey();
					JsonNode cols = node.getValue();
					if(cols.has(MessagingUtils.FIELD_COLUMN_NAMES) && cols.get(MessagingUtils.FIELD_COLUMN_NAMES).isArray())
					{
						int columnCount = 0;
						List<String> columns = new ArrayList<String>();
						for(JsonNode column : cols.get(MessagingUtils.FIELD_COLUMN_NAMES))
						{
							columnCount++;
							if(!column.asText().trim().equalsIgnoreCase(""))
							{
								columns.add(column.asText().trim());
							}
							else
							{
								throw new Exception("One of the Column Name in Column Family " + columnFamilyName + " is null.");
							}
						}
						if(columnCount <= 0)
						{
							throw new Exception("Column Names array is empty.");
						}
						appBean.getColumnFamilies().put(columnFamilyName.trim(), columns);
					}
					else
					{
						throw new Exception("Column Names are not in proper format.");
					}
					columnFamilyCount++;
				}
				if(columnFamilyCount <= 0)
				{
					throw new Exception("Column Family Names array is empty.");
				}
			}
			else
			{
				throw new Exception("Error in json structure for application structure.");
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			throw e;
		}
		return appBean;
	}

	private ApplicationBean parseGetJson(ObjectNode jsonNode) throws Exception 
	{
		ApplicationBean appBean;
		try
		{
			if(jsonNode.has(MessagingUtils.FIELD_APPLICATION_NAME) && !jsonNode.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim().equalsIgnoreCase("")
					&& jsonNode.has(MessagingUtils.FIELD_ROWKEY) && !jsonNode.get(MessagingUtils.FIELD_ROWKEY).asText().trim().equalsIgnoreCase("")
					&& jsonNode.has(MessagingUtils.FIELD_CF_LIST))
			{
				appBean = new ApplicationBean(jsonNode.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim());
				appBean.setRowKey(jsonNode.get(MessagingUtils.FIELD_ROWKEY).asText().trim());
				int columnFamilyCount = 0;
				JsonNode cfs = jsonNode.get(MessagingUtils.FIELD_CF_LIST);
				Iterator<Entry<String,JsonNode>> nodes = cfs.getFields();
				while(nodes.hasNext())
				{
					Entry<String, JsonNode> node = nodes.next();
					String columnFamilyName = node.getKey();
					JsonNode cols = node.getValue();
					if(cols.has(MessagingUtils.FIELD_COLUMN_NAMES) && cols.get(MessagingUtils.FIELD_COLUMN_NAMES).isArray())
					{
						int columnCount = 0;
						List<String> columns = new ArrayList<String>();
						for(JsonNode column : cols.get(MessagingUtils.FIELD_COLUMN_NAMES))
						{
							columnCount++;
							if(!column.asText().trim().equalsIgnoreCase(""))
							{
								columns.add(column.asText().trim());
							}
							else
							{
								throw new Exception("One of the Column Name in Column Family " + columnFamilyName + " is null.");
							}
						}
						if(columnCount <= 0)
						{
							throw new Exception("Column Names array is empty.");
						}
						appBean.getColumnFamilies().put(columnFamilyName.trim(), columns);
					}
					else
					{
						throw new Exception("Column Names are not in proper format.");
					}
					columnFamilyCount++;
				}
				if(columnFamilyCount <= 0)
				{
					throw new Exception("Column Family Names array is empty.");
				}
			}
			else
			{
				throw new Exception("Error in json structure for application structure.");
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			throw e;
		}
		return appBean;
	}
	
	private ApplicationBean parseSetJson(ObjectNode jsonNode) throws Exception 
	{
		ApplicationBean appBean = null;
		try
		{
			if(jsonNode.has(MessagingUtils.FIELD_APPLICATION_NAME) && !jsonNode.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim().equalsIgnoreCase("")
					&& jsonNode.has(MessagingUtils.FIELD_ROWKEY) && !jsonNode.get(MessagingUtils.FIELD_ROWKEY).asText().trim().equalsIgnoreCase("")
					&& jsonNode.has(MessagingUtils.FIELD_CF_LIST))
			{
				appBean = new ApplicationBean(jsonNode.get(MessagingUtils.FIELD_APPLICATION_NAME).asText().trim());
				appBean.setRowKey(jsonNode.get(MessagingUtils.FIELD_ROWKEY).asText().trim());
				int columnFamilyCount = 0;
				JsonNode cfs = jsonNode.get(MessagingUtils.FIELD_CF_LIST);
				Iterator<Entry<String,JsonNode>> nodes = cfs.getFields();
				while(nodes.hasNext())
				{
					columnFamilyCount++;
					Entry<String, JsonNode> node = nodes.next();
					String columnFamilyName = node.getKey();
					if(!columnFamilyName.trim().equalsIgnoreCase(""))
					{
						JsonNode cols = node.getValue();
						Iterator<Entry<String,JsonNode>> columnKVs = cols.getFields();
						int colCount = 0;
						while(columnKVs.hasNext())
						{
							Entry<String, JsonNode> columnKV = columnKVs.next();
							String colName = columnKV.getKey();
							if(!colName.trim().equalsIgnoreCase(""))
							{
								String colValue = columnKV.getValue().asText();
								if(appBean.getColumns().containsKey(columnFamilyName))
								{
									appBean.getColumns().get(columnFamilyName).put(colName, colValue);	
								}
								else
								{
									Map<String, String> columnValueMap = new HashMap<String, String>();
									columnValueMap.put(colName, colValue);
									appBean.getColumns().put(columnFamilyName, columnValueMap);
								}
							}
							else
							{
								throw new Exception("Column name in column family " + columnFamilyName + "is empty");
							}
							colCount++;
						}	
						if(colCount <= 0)
						{
							throw new Exception("Column Names not given for column Family " + columnFamilyName + " is empty.");
						}
					}
					else
					{
						throw new Exception("Column family name missing.");
					}
				}
				if(columnFamilyCount <= 0)
				{
					throw new Exception("No Column Family provided for update.");
				}
			}
			else
			{
				throw new Exception("Error in set query format.");
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
			throw e;
		}
		return appBean;
	}

	private void schemaRegistering(ObjectNode jsonNode, ApplicationBean appBean) throws Exception
	{
		requestID++;
		String appname = appBean.getAppName();
		File file = new File(Configurations.ROOT_DIRECTORY+appname);
		if (!file.exists()) 
		{
			if (file.mkdir()) 
			{
				try  
				{
					FileWriter jsonFile = new FileWriter(Configurations.ROOT_DIRECTORY+appname+"/"+appname+".json");
					jsonFile.write(jsonNode.toString());
					jsonFile.close();
					jsonNode.put(MessagingUtils.FIELD_REQUEST_ID, requestID);
					int count = 1;
					for(String sc : master.getNodeManager().getClientSockets().keySet())
					{
						jsonNode.put(MessagingUtils.FIELD_JOB_ID, count++);
						master.getNodeManager().send(sc, jsonNode);
					}
					ResponseHandler.responses.put(requestID
							, new ResponseBean(count-1, MessagingUtils.OPERATION_REGISTER, master, ResponseHandler.responses ,requestID));
				}
				catch(Exception e)
				{
					throw new Exception("Writing schema to master failed");
				}
			} 
			else 
			{
				throw new Exception("Making application directory failed");
			}
		}
		else
		{
			throw new Exception("Application directory already exists");
		}
	}

	private void schemaUnregistering(ObjectNode jsonNode) throws Exception
	{
		requestID++;
		File direc = new File(Configurations.ROOT_DIRECTORY+jsonNode.get(MessagingUtils.FIELD_APPLICATION_NAME).asText());
		if (direc.exists()) 
		{
			Boolean b = deleteFolder(direc);
			if(!b)
			{
				throw new Exception("Error in unregistering application.");
			}
			jsonNode.put(MessagingUtils.FIELD_REQUEST_ID, requestID);
			int count = 1;
			for(String sc : master.getNodeManager().getClientSockets().keySet())
			{
				jsonNode.put(MessagingUtils.FIELD_JOB_ID, count++);
				master.getNodeManager().send(sc, jsonNode);
			}
			ResponseHandler.responses.put(requestID,
					new ResponseBean(count-1, MessagingUtils.OPERATION_UNREGISTER, master, ResponseHandler.responses ,requestID));
		} 
		else 
		{
			throw new Exception("Application does not exist.");
		}
	}	

	private boolean deleteFolder(File direc)
	{
		try
		{
			File[] files = direc.listFiles();
			if(files!=null) 
			{ 
				for(File f: files)
				{
					if(f.isDirectory()) 
					{
						deleteFolder(f);
					} 
					else 
					{
						f.delete();
					}
				}
			}
			direc.delete();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public void processRequests()
	{
		System.out.println("Checking request queue");
		while(true)
		{	
			if(!requestQueue.isEmpty())
			{
				System.out.println("Checking request queue not empty");
				ObjectNode request = requestQueue.poll();
				if(request != null)
				{
					requestHandler(request);
				}
			}
			try 
			{
				Thread.sleep(100);
			} 
			catch (InterruptedException e)
			{
				e.printStackTrace();
				break;
			}
		}
	}
	
	@Override
	public void run() 
	{
		processRequests();
	}

	public void addSchemas(String node) 
	{
		try
		{
			File folder = new File(Configurations.ROOT_DIRECTORY);
			for (File fileEntry : folder.listFiles()) 
			{
		        if (fileEntry.isDirectory()) 
		        {
		        	ObjectNode jsonNode = (ObjectNode) mapper.readTree(new File(Configurations.ROOT_DIRECTORY + fileEntry.getName() + "/" + fileEntry.getName() + ".json"));
					master.getNodeManager().send(node, jsonNode);
		        }
		    }
		}
		catch(Exception e)
		{
			System.out.println("Failed to add schemas to newly added node : " + node);
			e.printStackTrace();
		}
	}
}