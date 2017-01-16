package com.lib.ds.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import com.lib.networking.SocketChannelComm;

public class ChatClient
{
	static HashMap<Integer, String> hm = new HashMap<Integer, String>();
	
	static void initialize() {
		hm.put(1, "register");
		hm.put(2, "unregister");
		hm.put(3, "get");
		hm.put(4, "set");
		hm.put(5, "delete");
	}
	
	public static void main(String args[])throws IOException
	{
		initialize();
		String applicationName, result = "";
		int operation = -1;
		ObjectMapper mapper = new ObjectMapper();
		ObjectNode read;		
		BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
		
		SocketChannelComm client = new SocketChannelComm();
		client.connect("192.168.0.11", 9001);
		Selector readSel = Selector.open();
		client.initSocket(readSel, null);
	
		do {
			ObjectNode rootNode = mapper.createObjectNode();
			
			result = "";
			System.out.println("Please enter application name: ");
			applicationName = input.readLine();	
			rootNode.put("appName", applicationName);
			
			System.out.println("Enter type of operation:");
			System.out.println("1) Register application \n 2) Unregister application \n 3) Get \n 4) Set \n 5) Delete");
			try {
				operation = Integer.parseInt(input.readLine());
			}
			catch (NumberFormatException e) {
				System.out.print(e);
				return;
			}
			
			if(operation < 1 || operation > 5) {
				System.out.println("Invalid choice");
				continue;
			}
			else {
				rootNode.put("operation", hm.get(operation));
			}
			
			if(operation == 1) {		
				System.out.println("Enter number of column families");
				int columnFamiliesCount = Integer.parseInt(input.readLine());
				int columnFamilyCounter = 0;
				ObjectNode columnFamiliesNode = mapper.createObjectNode();
				while(columnFamilyCounter++ < columnFamiliesCount) {
					ObjectNode columnFamilyNode = mapper.createObjectNode();
					System.out.println("Enter name of column family " +columnFamilyCounter);
					String columnFamilyName = input.readLine();
					System.out.println("Enter number of columns in "+columnFamilyName);
					int columnCounter = 0;
					int columnCount = Integer.parseInt(input.readLine());
					ArrayNode columnNamesNode = mapper.createArrayNode();
					while(columnCounter++ < columnCount) {
						System.out.println("Enter name of column "+columnCounter);
						String columnName = input.readLine();
						columnNamesNode.add(columnName);
					}
					columnFamilyNode.put("columnNames", columnNamesNode);
					columnFamiliesNode.put(columnFamilyName, columnFamilyNode);
				}
				rootNode.put("columnFamilies", columnFamiliesNode);
			}
			else if(operation == 3 || operation == 4 || operation == 5) { // get, set, delete
				System.out.println("Enter rowKey");
				String rowKey = input.readLine();
				rootNode.put("rowKey", rowKey);
				
				if(operation == 3) { // get
					System.out.println("Enter number of column families");
					int columnFamiliesCount = Integer.parseInt(input.readLine());
					int columnFamilyCounter = 0;
					ObjectNode columnFamiliesNode = mapper.createObjectNode();
					while(columnFamilyCounter++ < columnFamiliesCount) {
						ObjectNode columnFamilyNode = mapper.createObjectNode();
						System.out.println("Enter name of column family " +columnFamilyCounter);
						String columnFamilyName = input.readLine();
						System.out.println("Enter number of columns in "+columnFamilyName);
						int columnCounter = 0;
						int columnCount = Integer.parseInt(input.readLine());
						ArrayNode columnNamesNode = mapper.createArrayNode();
						while(columnCounter++ < columnCount) {
							System.out.println("Enter name of column "+columnCounter);
							String columnName = input.readLine();
							columnNamesNode.add(columnName);
						}
						columnFamilyNode.put("columnNames", columnNamesNode);
						columnFamiliesNode.put(columnFamilyName, columnFamilyNode);
					}
					rootNode.put("columnFamilies", columnFamiliesNode);
				}
				else if (operation == 4) { // set
					System.out.println("Enter number of column families");
					int columnFamiliesCount = Integer.parseInt(input.readLine());
					int columnFamilyCounter = 0;
					ObjectNode columnFamiliesNode = mapper.createObjectNode();
					while(columnFamilyCounter++ < columnFamiliesCount) {
						System.out.println("Enter name of column family " +columnFamilyCounter);
						String columnFamilyName = input.readLine();
						System.out.println("Enter number of columns in "+columnFamilyName +" to set value in:");
						int columnCounter = 0;
						int columnCount = Integer.parseInt(input.readLine());
						ObjectNode columnNamesNode = mapper.createObjectNode();
						while(columnCounter++ < columnCount) {
							System.out.println("Enter name of column "+columnCounter);
							String columnName = input.readLine();
							System.out.println("Enter value ");
							String value = input.readLine();
							columnNamesNode.put(columnName, value);
						}
						columnFamiliesNode.put(columnFamilyName, columnNamesNode);
					}
					rootNode.put("columnFamilies", columnFamiliesNode);			
				}
			}
			//result = "{\"appName\": \"app_name\",\"operation\": \"register\",\"columnFamilies\": [\"name\", \"address\"],\"name\": [\"firstname\", \"lastname\"],\"address\": [\"street\", \"city\", \"state\", \"country\", \"pincode\"]}";
			int length = rootNode.toString().length()+1;
			result = length + "#" + rootNode.toString();
			System.out.println(result);
			client.sendJSON(rootNode);

			readSel.select(10000);
			boolean[] deleteKey = {false};
			Set<SelectionKey> keys = readSel.selectedKeys();
			Set<SelectionKey> removeKeys = new HashSet<SelectionKey>();
			for(SelectionKey key : keys){
				if(key.isReadable()){
					read = client.receiveJSON(deleteKey);
					System.out.println("Read conversion");
					System.out.println(read.toString());
					//System.out.println("DeleteKey:"+deleteKey[0]);
					if(deleteKey[0]){
						removeKeys.add(key);
					}
					if(read != null){
						System.out.println("Received:"+read.toString());
					}
				}
			}
			for(SelectionKey key : removeKeys){
				keys.remove(key);
			}
			
			// System.out.println(result);
//			byte bt[] = result.getBytes("UTF-16");
//			byte btSplice[] = Arrays.copyOfRange(bt, 2, bt.length);
//			System.out.print(Arrays.toString(btSplice));
//			BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
//			String serverResponse;
//			PrintStream ps = new PrintStream(s.getOutputStream());
//			System.out.println("Client Message: " +result);
////			ps.println(Arrays.toString(btSplice));
//			serverResponse = br.readLine();
//			System.out.println("Server Message: " +serverResponse);
		} while(!result.equalsIgnoreCase("bye"));
		
//		s.close();
	}
}