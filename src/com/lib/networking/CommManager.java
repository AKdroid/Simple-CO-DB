package com.lib.networking;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Set;
import org.codehaus.jackson.node.ObjectNode;

import com.lib.ds.utils.SHashMap;

public class CommManager implements Runnable {

	SHashMap<String, SocketChannelComm> clientSockets;
	ServerComm serverSock;
	int port;
	String hostname;	
	boolean accepting;
	Selector readsel;
	
    public CommManager(boolean isListen, String host, int port) throws IOException{
    	hostname = host;
    	this.port = port;
		clientSockets = new SHashMap<String, SocketChannelComm>();
		if(isListen) 
			serverSock = new ServerComm(hostname,port);
		accepting  = true;
		readsel = Selector.open();
	}
	
	public void listen(){
		System.out.println("CommManager:: Accepting Connections on port:"+port);
		while(accepting){
			SocketChannelComm client = serverSock.accept(readsel, null);
			try {
				String address = client.getSocketChannel().getRemoteAddress().toString();
				clientSockets.put(address, client);
				System.out.println("CommManager:: Accepted connection from address:"+address);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			cleanup();
			
		}
	}
	
	public void stopLisxtening(){
		accepting = false;
	}

	@Override
	public void run() {
		if(serverSock != null)
			listen();
	}
	
	public ReadData readNext(long timeout){
		
		ReadData result = null;
		try {
			readsel.select(timeout);
			Set<SelectionKey> keys = readsel.selectedKeys();
			boolean[] doDelete = {false};
			SelectionKey toDelete = null;
			for(SelectionKey key : keys){
				if(key.isReadable()){
					SocketChannel sock = (SocketChannel) key.channel();
					String address = sock.getRemoteAddress().toString();
					SocketChannelComm client = clientSockets.get(address);
					result = new ReadData();
					result.data = client.receiveJSON(doDelete);
					if(result.data!=null)
						System.out.println("CommManager:: Received from "+ address +":"+result.data.toString());
					result.from = address;
					toDelete = key;
					break;
				}
			}
			if(doDelete[0])
				keys.remove(toDelete);
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		
		return result;
	}
	
	public boolean send(String address, ObjectNode data){
		boolean result = false;
		if(clientSockets.containsKey(address)){
			System.out.println("CommManager:: Sending to "+address+":"+data.toString());
			result = clientSockets.get(address).sendJSON(data);
		}
		else
			result = false;
		return result;
	}
	
	public boolean connect(String hostName, int port, String name){
		try {
			SocketChannelComm client = new SocketChannelComm();
			System.out.println("CommManager:: Attempting to connect to " + name +"::"+ hostName + ":" + port );
			client.connect(hostName, port);
			client.initSocket(readsel, null);
			clientSockets.put(name,client);
			System.out.println("CommManager:: Connected to address:"+client.getSocketChannel().getRemoteAddress().toString());
			clientSockets.put(client.getSocketChannel().getRemoteAddress().toString(), client);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public boolean isConnected(String address){
		if(clientSockets.containsKey(address)){
			return clientSockets.get(address).getSocketChannel().isConnected();
		}
		return false;
	}
	
	public void cleanup(){
		LinkedList<String> toBeRemoved = new LinkedList<String>();
		for(String address: clientSockets.keySet()){
			if(! isConnected(address)){
				toBeRemoved.add(address);
			}
		}
		for(String key: toBeRemoved){
			System.out.println("CommManager:: Removing Inactive connection:"+ key);
			clientSockets.remove(key);
		}
	}
	
	public Set<String> keySet(){
		return clientSockets.keySet();
	}
	
	public String getLocalIpAddress(){
		try {
			if(clientSockets.containsKey("master"))
				return clientSockets.get("master").getSocketChannel().getLocalAddress().toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String getLocalAddress(String key){
		if(clientSockets.containsKey(key))
			try {
				return clientSockets.get(key).getSocketChannel().getLocalAddress().toString();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
	}
	
	public String getRemoteAddress(String key){
		if(clientSockets.containsKey(key))
			try {
				return clientSockets.get(key).getSocketChannel().getRemoteAddress().toString();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
	}

	public SHashMap<String, SocketChannelComm> getClientSockets() {
		// TODO Auto-generated method stub
		return clientSockets;
	}
}
