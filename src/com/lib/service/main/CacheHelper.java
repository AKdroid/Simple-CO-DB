package com.lib.service.main;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;


public class CacheHelper<K,V> {

	Cache<K,V> cache;
	
	public CacheHelper( int maxEntries){
		@SuppressWarnings("unchecked")
		CacheBuilder<K,V> builder = (CacheBuilder<K, V>) CacheBuilder.newBuilder().maximumSize(maxEntries);
		cache = builder.build();
	}
	
	public V get(K key){
		V value;
		value = cache.getIfPresent(key);
		System.out.println("Cache: Get "+key);
		return value;
	}
	
	public void put(K key, V value){
		cache.put(key, value);
		System.out.println("Cache: Put "+key);
	}
	
	public void invalidate(K key){
		System.out.println("Cache: Invalidate "+key);
		cache.invalidate(key);
	}
	
	public void monitorStats(){
		CacheStats s = cache.stats();
		System.out.println("Hit-Rate:"+s.hitRate());
	}
	
	
}
