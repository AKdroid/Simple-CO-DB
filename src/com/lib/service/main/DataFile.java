package com.lib.service.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.lib.ds.utils.MessagingUtils;

public class DataFile {
	
	BloomFilter<String> filter;
	BufferedReader reader;
	BufferedWriter writer;
	String path;
	String[] columnNames;
	String columnLine;
	
	@SuppressWarnings("unchecked")
	public DataFile(String path) throws NumberFormatException, IOException, ClassNotFoundException{
		this.path = path;
		initFile(path);
		if(new File(path+".filter").isFile()){
			filter = (BloomFilter<String>) new ObjectInputStream(
			        new FileInputStream(path+".filter")).readObject();
		} else {
			filter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),40000);
		}
	}
	
	public DataFile(String path, String[] columnNames) throws IOException{
		this.path = path;
		createNewFile(path, columnNames);
		initFile(path);
		filter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),40000);
	}
	
	private void createNewFile(String path, String[] columnNames) throws IOException{
		this.path = path;
		if(!new File(path).isFile()){
			writer = new BufferedWriter(new OutputStreamWriter
					(new FileOutputStream(path), StandardCharsets.UTF_8));
			writer.write(MessagingUtils.FIELD_ROWKEY_LOCAL+",Active");
			for(String s : columnNames){
				writer.write(","+s);
			}
			writer.write("\n");
			writer.close();
			System.out.println("DataFile:: Creating new file: "+ path);
		}
	}
	
	private void initFile(String path) throws NumberFormatException, IOException{
		reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(path),StandardCharsets.UTF_8));
		columnLine = reader.readLine();
		columnNames = columnLine.split(",");
		reader.close();
		
	}
	
	public Record get(String rowKey) throws IOException{
		Record rec = null;
		initReader();
		
		if(filter.mightContain(rowKey)){
			String line;
			while((line = reader.readLine()) != null){
				rec = new Record(columnNames, line);
				String key = rec.getData(MessagingUtils.FIELD_ROWKEY_LOCAL);
				if(key.equals(rowKey)){
					System.out.println("Datafile:: Found record with key: "+rowKey +" in file:" + path);
					break;
				}
			}
		} else {
			System.out.println("Datafile:: Skipping search. BloomFilter indicates absence of key: "
						+rowKey +" in file:" + path);
		}
		
		reader.close();
		return rec;
		
	}
	
	public Record set(String rowKey, HashMap<String,String> values) throws IOException{
		Record rec=null,temp = null;
		boolean edited = false;
		System.out.println("Datafile:"+rowKey);
        if(filter.mightContain(rowKey)){
            writeHeader(path+".temp");
            initReader();
            String line;
            while((line = reader.readLine()) != null){
                temp = new Record(columnNames, line);
                String key = temp.getData(MessagingUtils.FIELD_ROWKEY_LOCAL);
                if(key.equals(rowKey)){
                	rec = new Record(columnNames, line);
                	edited = true;
                    for(String k : values.keySet()  ){
                    	if(!rec.setData(k, values.get(k))){
                    		rec = null;
                    		edited = false;
                    		break;
                    	}
                    }
                    if(!edited){
                    	break;
                    }
                    writer.write(rec.toString().trim() + "\n");
                    System.out.println("Datafile:: Updated record with key: "+rowKey +" in file:" + path);
                } else {
                    writer.write(line.trim()+"\n");
                }
            }
            writer.close();
        }
        
        reader.close();
        if(!edited){	
        	new File(path+".temp").delete();
        } else {
        	new File(path+".temp").renameTo(new File(path));
        	filter.put(rowKey);
        	writeFilter();
        }
        return rec;
	}
	
	public Record append(String rowKey, HashMap<String, String> values ) throws IOException{
		Record rec = new Record(columnNames);
		
		rec.setData(MessagingUtils.FIELD_ROWKEY_LOCAL, rowKey);
		rec.setData("Active", "1");
		for(String column : values.keySet()){
			if(!rec.setData(column, values.get(column)) ){
				return null;
			}
		}
		filter.put(rowKey);
		System.out.println(path);
		initReader();
		writeHeader(path+".temp");
		String line;
		while((line = reader.readLine())!= null){
			writer.write(line.trim()+"\n");
		}
		writer.write(rec.toString());
		writer.close();
		reader.close();
		System.out.println("Datafile:: Inserted record with key: "+rowKey+" in file:" + path);
		new File(path+".temp").renameTo(new File(path));
		writeFilter();
		return rec;
	}
	
	public void initReader() throws IOException{
		reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(path),StandardCharsets.UTF_8));
		reader.readLine();
	}
	
	public void writeHeader(String p) throws IOException{
		writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(p), StandardCharsets.UTF_8));
		writer.write(columnLine+"\n");
	}
	
	public void writeFilter() throws FileNotFoundException, IOException{
		ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(
		        path+".filter"));
		stream.writeObject(filter);
		stream.close();
	}
		
	public boolean delete(String rowKey) throws IOException{
		BloomFilter<String> newFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),40000);
		Record rec = null;
		writeHeader(path+".temp");
		String line;
		initReader();
		boolean result = false;
		while ( (line = reader.readLine()) != null  ){
			rec = new Record(columnNames, line);
			String key = rec.getData(MessagingUtils.FIELD_ROWKEY_LOCAL);
			if(key != null && key.equals(rowKey)){
				result = true;
			} else {
				newFilter.put(key);
				writer.write(line.trim()+"\n");
			}
		}
		writer.close();
		reader.close();
		if(result){
			new File(path+".temp").renameTo(new File(path));
			filter = newFilter;
			writeFilter();
			System.out.println("Datafile:: Deleted record with key: "+rowKey +" in file:" + path);
		} else {
			new File(path+".temp").delete();
		}
		return result;
	}
}
