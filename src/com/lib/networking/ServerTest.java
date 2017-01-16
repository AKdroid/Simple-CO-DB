package com.lib.networking;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;



public class ServerTest implements Runnable {

	ServerComm server;
	Selector readsel;
	HashMap<SocketChannel,SocketChannelComm> clients;
	
	public ServerTest(String host, int port) throws IOException{
		server = new ServerComm("0.0.0.0",9000);
		readsel = Selector.open();
		clients = new HashMap<SocketChannel,SocketChannelComm>();
	}
	
	public void read() throws IOException, InterruptedException{
		SocketChannel temp;
		ObjectNode read;
		ObjectNode AckJSON = new ObjectMapper().createObjectNode();
		AckJSON.put("messageType", "0");
		AckJSON.put("message", "Acknowledged");
		while(true){
			//read if something new
			readsel.select(100);
			Set<SelectionKey> keys = readsel.selectedKeys();
			Set<SelectionKey> removeKeys = new HashSet<SelectionKey>();
			boolean[] deleteKey = {false};
			for(SelectionKey key : keys){
				
				if(key.isReadable()){
					//synchronized(clients){
						temp = (SocketChannel) key.channel();
						read = clients.get(temp).receiveJSON(deleteKey);
						if(deleteKey[0])
							removeKeys.add(key);
						if(read != null){
							System.out.println("Received:"+read.toString());
							System.out.println("DeleteKey:"+deleteKey[0]);
						}
						clients.get(temp).sendJSON(AckJSON);
					}
				//}
			}
			for(SelectionKey key : removeKeys){
				keys.remove(key);
			}
			Thread.sleep(1000);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		ServerTest s = new ServerTest("0.0.0.0",9000);
		new Thread(s).start();
		
		Thread.sleep(10000);
		
		s.read();
		
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			System.out.println("Listening for connections");
			SocketChannelComm client = server.accept(readsel, null);
			try {
				System.out.println("Accepted connection:"+client.getSocketChannel().getRemoteAddress().toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			//synchronized(clients){l
				if(client != null)
					clients.put(client.getSocketChannel(), client);
			//}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
