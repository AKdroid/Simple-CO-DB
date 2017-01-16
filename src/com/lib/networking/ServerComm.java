package com.lib.networking;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class ServerComm{

	ServerSocketChannel server;
	Selector acceptor;
	
	public ServerComm(String hostname, int port) throws IOException{
		server = ServerSocketChannel.open();
		server.configureBlocking(false);
		InetSocketAddress listenerAddress = new InetSocketAddress(hostname,port);
		server.bind(listenerAddress);
		acceptor = Selector.open();
		server.register(acceptor, SelectionKey.OP_ACCEPT);
	}
	
	public SocketChannelComm accept(Selector readSelect, Selector writeSelect){
		
		SocketChannelComm comm = null;
		try {
			acceptor.select();
			Set<SelectionKey> keys = acceptor.selectedKeys(); 
			SelectionKey toRemove = null;
			for(SelectionKey key : keys){
				if(key.isAcceptable()){
					ServerSocketChannel sock = (ServerSocketChannel) key.channel();
					SocketChannel clientSocket = sock.accept();
					comm = new SocketChannelComm(clientSocket);
					comm.initSocket(readSelect,writeSelect);
					toRemove = key;
					break;
				}
			}
			keys.remove(toRemove);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			comm = null;
			e.printStackTrace();
		}
		return comm;
	}
	
}
