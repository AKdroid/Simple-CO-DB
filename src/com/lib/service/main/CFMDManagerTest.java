package com.lib.service.main;

import java.util.HashMap;
import java.util.Random;

public class CFMDManagerTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CFMetadataManager manager = new CFMetadataManager("appName", "Address","/home/akhil/test/Address/", 
				new String[]{"Street","City","State","Zip"},10);
		String rowKeyStub = "appName#Address#row";
		HashMap<String, String> data = new HashMap<String,String>();
		
		String street = "2820 Avent Ferry Road Apt-";
		String city = "Raleigh";
		String state = "NC";
		String zip = "27606";
		Record rec;
		data.put("City", city);
		data.put("State", state);
		data.put("Zip", zip);
		for(int i=0;i<100;i++){
			data.put("Street",street+i);
			int groupNum=i%2;
			CFRecord record = manager.set(rowKeyStub+i, groupNum, data, -1, false, true);
			if(record != null){
				rec = record.record;
				if(rec != null){
					System.out.println("File no.: "+record.sourceFile);
					System.out.println(rec.toString());
				} 
			} else {
				System.out.println("Set failed");
			}
		}

		System.out.println("------getTest");
		for(int i=0;i<100;i++){
		CFRecord r = manager.get(rowKeyStub+i, i%2);
		if(r!=null){
			System.out.println("File no.: "+r.sourceFile);
			System.out.println(r.record.toString());
		}
		}
		System.out.println("---Set test");
		
		Random random = new Random();
		for(int j=0;j<30;j++){
			int i = random.nextInt(200);
			int groupNum = i%2;
			data.put("Street", street+(200+i));
			CFRecord record = manager.set(rowKeyStub+i, groupNum, data, -1, false, false);
			if(record != null){
				rec = record.record;
				if(rec != null){
					System.out.println(rec.toString());
				} 
			} else {
				System.out.println("Set failed");
			}
		}
		/*
		System.out.println("Delete test");
		//delete test
		for(int j=0;j<10;j++){
			int x = random.nextInt(50);
			int i=50+random.nextInt(200);
			int groupNum = i%2;
			CFRecord r;
			r = manager.delete(rowKeyStub+x, x%2);
			if(r!=null){
				System.out.println("Deleted from file:"+r.sourceFile+" "+x);
			}
			data.put("Street",street+i+1000);
			r = manager.set(rowKeyStub+i, groupNum, data);
			if(r != null){
				rec = r.record;
				if(rec != null){
					System.out.println("File no.: "+r.sourceFile);
					System.out.println(rec.toString());
				} 
			}
		}*/
		
	}

}
