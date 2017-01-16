package com.lib.ds.utils;

import java.util.HashMap;
import java.util.Set;

public class SHashMap<K,V> {

	HashMap<K,V> map;
	
	public SHashMap(){
		map = new HashMap<K,V>();
	}
	
	public synchronized V get(K key){
		V value = null;
		if(map.containsKey(key))
			value = map.get(key);
		return value;
	}
	
	public synchronized void put(K key, V value){
		value = map.put(key,value);
	}
	
	public synchronized boolean containsKey(K key){
		return map.containsKey(key);
	}
	
	public synchronized void remove(K key){
		map.remove(key);
	}
	
	public synchronized Set<K> keySet(){
		return map.keySet();
	}
}
