package com.lib.networking;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;


public class SocketChannelComm {
	
	SocketChannel socket;
	ObjectMapper mapper;
	String buffer;
	int sum;
	
	public SocketChannelComm(SocketChannel sock){
		this.socket = sock;
		mapper = new ObjectMapper();
		buffer = "";
		sum = 0;
	}
	
	public SocketChannelComm() throws IOException{
		mapper = new ObjectMapper();
		buffer = "";
	}
	
	public void initSocket(Selector readSel,Selector writeSel) throws IOException{
		socket.configureBlocking(false);
		if(readSel != null)
			socket.register(readSel, SelectionKey.OP_READ);
		if(writeSel != null){
			socket.register(writeSel, SelectionKey.OP_WRITE);
		}
	}
	
	public SocketChannel getSocketChannel(){
		return socket;
	}
	
	public void connect(String host, int port){
		InetSocketAddress remote = new InetSocketAddress(host,port);
		while(true){
			try {
				socket = SocketChannel.open();
				socket.configureBlocking(false);
				System.out.println("SocketChannelComm:: Attempting to connect to :: " + host + ":" + port);
				socket.connect(remote);
				while(!socket.finishConnect()){
					Thread.sleep(2000);
				}
				System.out.println("SocketChannelComm:: Connected to :: " + host + ":" + port);
			} catch (IOException e) {
				socket = null;
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(socket!=null)
				break;
			try {
				Thread.sleep(3000);
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}				
		}
	}
	
	public synchronized boolean sendJSON(ObjectNode node){
		if(node == null)
			return false;
		String json = node.toString();
		String toSend = (json.length()+1)+"#"+json;
		byte[] buffer = toSend.getBytes(StandardCharsets.UTF_16);
		ByteBuffer buf = ByteBuffer.wrap(buffer,2,buffer.length-2);
		try {
			while(buf.hasRemaining()){
				socket.write(buf);
				
			}
			buf.clear();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		System.out.println("SocketChannelComm:: Sending JSON "+toSend);
		return true;
	}
	
	public ObjectNode receiveJSON(boolean[] deleteKey){
		ObjectNode result = null;
		String read = null;
		int length = -1;
		ByteBuffer buf = ByteBuffer.allocate(1024*1024); //1MB byte buffer
		int bytesRead = 0;
		//System.out.println("buffer:"+buffer+":"+buffer.length());
		//System.out.println("sum:"+sum);
		try {
			bytesRead = socket.read(buf);
			while(bytesRead > 0){
				sum+=bytesRead;
				buf.flip();
				byte[] b = new byte[bytesRead+2];
				b[0]=-2;
				b[1]=-1;
				buf.get(b, 2, bytesRead);
				buffer+=new String(b,StandardCharsets.UTF_16);
				buf.clear();
				bytesRead = socket.read(buf);
			}
			//System.out.println("buffer:"+buffer+":"+buffer.length());
			//System.out.println("sum:"+sum);
			if(bytesRead == -1){
				deleteKey[0] = true;
				System.out.println("SocketChannelComm:: Closed socket ");
				socket.close();
			}
			int sepIndex = buffer.indexOf('#');
			if(sepIndex > -1){
				length = Integer.parseInt(buffer.substring(0,sepIndex));
				read = buffer.substring(0,sepIndex+length);
				sum = sum-2*(read.length());
				if(sum > 0 )
					deleteKey[0] = false;
				else
					deleteKey[0] = true;
				read = read.substring(sepIndex);
				if(length == -1 || read.length() < length)
					return null;
				if(bytesRead == -1){
					deleteKey[0] = true;
					return null;
				}
				read = read.substring(1,length);
				result = (ObjectNode)mapper.readTree(read);
				buffer = buffer.substring(sepIndex+length);
			}
			if(bytesRead == -1){
				deleteKey[0] = true;
			}
			
		} catch (Exception e){
			e.printStackTrace();
		}
		if(result != null){
			System.out.println("SocketChannelComm :: Received JSON "+result.toString());
		}
		return result;
	}
}
