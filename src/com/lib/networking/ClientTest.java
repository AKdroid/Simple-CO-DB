package com.lib.networking;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class ClientTest {
	
	public static void main(String[] args) throws IOException {
		SocketChannelComm client = new SocketChannelComm();
		client.connect("localhost", 9000);
		Selector readSel = Selector.open();
		client.initSocket(readSel, null);
		ObjectNode helloJSON = new ObjectMapper().createObjectNode();
		helloJSON.put("messageType", 1);
		helloJSON.put("message", "Hello Server\n");
		ObjectNode read;
		client.sendJSON(helloJSON);
		int cnt=5;
		boolean rd = false;
		while(cnt>0){
			//read if something new
			if(!rd){
			client.sendJSON(helloJSON);
			rd = true;
			}
			readSel.select(100);
			boolean[] deleteKey = {false};
			Set<SelectionKey> keys = readSel.selectedKeys();
			Set<SelectionKey> removeKeys = new HashSet<SelectionKey>();
			for(SelectionKey key : keys){
				if(key.isReadable()){
					rd = false;
					read = client.receiveJSON(deleteKey);
					System.out.println("DeleteKey:"+deleteKey[0]);
					cnt--;
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
		}

		

	}

}
