package com.lib.ds.utils;

import java.util.LinkedList;

public class PCQueue<T> {

	//ProducerConsumerQueue
	
	LinkedList<T> queue;
	int size;
	
	public PCQueue(int size){
		queue = new LinkedList<T>();
		this.size = size;
	}
	
	public boolean isEmpty(){
		return queue.size() == 0;
	}
	
	public synchronized boolean add(T data){
		if(size > 0 && queue.size() < size){
			queue.add(data);
			return true;
		}
		return false;
		
	}
		
	public synchronized T poll(){
		T top = null;
		if(queue.size() > 0)
			top = queue.remove(0);
		return top;
	}
	
	public synchronized T peek(){
		T top = null;
		if(queue.size() > 0)
			top = queue.get(0);
		return top;
	}
}
