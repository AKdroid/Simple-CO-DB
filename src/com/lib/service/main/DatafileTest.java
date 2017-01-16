package com.lib.service.main;

import java.io.IOException;
import java.util.HashMap;

import com.lib.ds.utils.MessagingUtils;

public class DatafileTest {
	
	public static void main(String[] args) throws IOException{
		
		DataFile d = new DataFile("/home/akhil/test/1024.csv",new String[]{"FirstName","LastName","MiddleName"});
		HashMap<String,String>  map = new HashMap<String,String>();
		String rowKey = "application#FamilyName#rownum";
		String name1 = "Akhil";
		String name2 = "Raghavendra";
		String name3 = "Rao";
		Record rec;
		for(int i=0;i<100;i++){
			map.clear();
			map.put(MessagingUtils.FIELD_ROWKEY_LOCAL, rowKey+i);
			map.put("FirstName",name1+i);
			map.put("MiddleName",name2+i);
			map.put("LastName",name3+i);
			rec = d.append(rowKey+i, map);
			if(rec != null){
				System.out.println(rec.toString());
			} else {
				System.out.println("No record found");
			}
		}
		
		rec = d.get(rowKey+50);
		if(rec != null){
			System.out.println(rec.toString());
			map.clear();
			map.put("FirstName",null);
			map.put("LastName","Rao;\n,akhil&nl;\\\"hsdadjka");
			d.set(rowKey + 51, map);
			rec = d.get(rowKey+51);
			if(rec!=null){
				System.out.println(rec.toString());
				System.out.println(rec.getData("LastName"));
			}
		} else {
			System.out.println("Failed to get record");
		}
			
		
	}
	

}
