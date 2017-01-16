package com.lib.service.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class CFMetadataManager {
	
	String appName;
	String name;
	String root;
	HashMap<Integer,Integer> rowCounts;
	HashSet<Integer> filled;
	public String[] columns;
	int filegroupSize;
	public final int MAX_RECORDS_COUNT = 20;
	
	public CFMetadataManager(String appName, String name, String root, String[] columnNames, int fgsize){
		this.name = name;
		this.appName = appName;
		this.root = root;
		if(this.root.charAt(this.root.length()-1)!='/')
			this.root+="/";
		System.out.println(this.root);
		this.columns = columnNames;
		rowCounts = new HashMap<Integer,Integer>();
		filled = new HashSet<Integer>();
		filegroupSize = fgsize;
		System.out.println("CFMetaDataManager:: name "+ name + " Initialized root to "+root);
		System.out.println("CFMetaDataManager:: name "+ name + "File group size: "+filegroupSize);
		initFromFile();
	}
	
	private void initFromFile(){
		
		try {
			File scriptsFolder = new File("scripts");
			
			String[] cmd = {
			        "/bin/bash",
			        "-c",
			        "python "+ scriptsFolder.getAbsolutePath() +"/rowCount.py '" + root + "'"
			    };
			Process p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new FileReader(root+".metadatarc"));
			String line;
			while((line = reader.readLine()) != null){
				String[] values = line.split(",");
				int fileNum = Integer.parseInt(values[0].trim());
				int count = Integer.parseInt(values[1].trim());
				rowCounts.put(fileNum, count);
				if(count == MAX_RECORDS_COUNT){
					filled.add(fileNum);
				}
			}
			reader.close();
		} catch (IOException | InterruptedException e) {
			System.out.println("rowCount.py failed to run");
			e.printStackTrace();
		}
	}
	
	public boolean existsFile(int fileNum){
		return rowCounts.containsKey(fileNum) && new File(root+fileNum+".csv").isFile();
	}
	
	public int getNonFilledFile(int groupNum){
		int fileNum = groupNum;
		while(existsFile(fileNum) && filled.contains(fileNum)){
			fileNum = fileNum + filegroupSize;
		}
		if(!existsFile(fileNum)){
			try {
				DataFile df = new DataFile(root+fileNum+".csv",columns);
				rowCounts.put(fileNum, 0);
			} catch (IOException e) {
				fileNum = -1;
				e.printStackTrace();
			}
		}
		return fileNum;
	}

	public CFRecord get(String rowKey, int groupNum){
		CFRecord record = null;
		Record rec = null;
		int file = groupNum;
		while(existsFile(file)){
			try {
				DataFile d = new DataFile(root+file+".csv");
				rec = d.get(rowKey);
				if(rec != null){
					record = new CFRecord();
					record.CFName = name;
					record.sourceFile = file;
					record.record = rec;
					break;
				}
			} catch (NumberFormatException e) {
				e.printStackTrace();
				break;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				break;
			} catch (IOException e) {			
				e.printStackTrace();
				break;
			}
			file = file + filegroupSize;
		}
		return record;
	}
	
	public CFRecord set(String rowKey, int groupNum, HashMap<String,String> values, 
			int fileNum, boolean forceModify, boolean forceAppend){
		CFRecord record = null;
		boolean isAppend=false;
		if(forceAppend){
			fileNum = getNonFilledFile(groupNum);
			isAppend = true;	
		} 
		if(fileNum <=-1 || forceModify){
			record = get(rowKey, groupNum);
			if(record != null){
				//System.out.println("ExistingKey:"+rowKey);
				fileNum = record.sourceFile;
			} else {
				if(!forceModify){
					fileNum = getNonFilledFile(groupNum);
					isAppend = true;
				} else {
					fileNum = -1;
				}
			}
		}
		if(fileNum < 0)
			return null;
		System.out.println("in set");
		try {
			DataFile df = new DataFile(root+fileNum+".csv");
			Record rec;
			if(!isAppend){
				rec = df.set(rowKey, values);
			}
			else
				rec = df.append(rowKey, values);
			if(rec != null){
				record = new CFRecord();
				if(isAppend){
					//appended
					rowCounts.put(fileNum, rowCounts.get(fileNum)+1);
					if(rowCounts.get(fileNum) == MAX_RECORDS_COUNT)
						filled.add(fileNum);
					record.isAppend = true;
				}
				record.sourceFile = fileNum;
				record.CFName = name;
				record.record = rec;
				
			} else {
				record = null;
			}
		} catch (NumberFormatException | ClassNotFoundException | IOException e) {
			record = null;
			e.printStackTrace();
		}
		return record;
	}
	
	public CFRecord delete(String rowKey, int groupNum, int fileNum){
		CFRecord record = null;
		if(fileNum == -1){
			record = get(rowKey, groupNum);
			if(record != null)
				fileNum = record.sourceFile;
		}
		if(fileNum != -1){
			//fileNum = record.sourceFile;
			try {
				DataFile df = new DataFile(root + fileNum+".csv");
				if( df.delete(rowKey)){
					record = new CFRecord();
					record.CFName = name;
					record.sourceFile = fileNum;
					record.record = null;
					rowCounts.put(fileNum, rowCounts.get(fileNum)-1);
					if(filled.contains(fileNum))
						filled.remove(fileNum);
				} else {
					record = null;
				}
			} catch (NumberFormatException | ClassNotFoundException | IOException e) {
				record = null;
				e.printStackTrace();
			}
		}
		return record;
	}
}
