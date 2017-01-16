package com.lib.service.main;

import java.util.HashMap;

import com.lib.ds.utils.MessagingUtils;

public class Record {

	HashMap<String, String> data;
	String[] layout;
	
	public Record(String[] layout){
		data = new HashMap<String,String>();
		this.layout = layout;
		for(int i=0; i<layout.length; i++){
			data.put(layout[i], null);
		}
	}
	
	public Record(String[] layout, String line){
		data = new HashMap<String,String>();
		this.layout = layout;
		String[] afterSplit = line.split("[^\\\\],");
		for(int i=0; i<layout.length; i++){
			String value = Record.deserializeString(afterSplit[i]);
			if(layout[i].equals(MessagingUtils.FIELD_ROWKEY_LOCAL))
				data.put("RowKey", value.split("#")[2]);
			data.put(layout[i], value);
		}
	}
	
	public boolean setData(String columnName, String value){
		if(data.containsKey(columnName)){
			data.put(columnName, value);
			return true;
		}
		return false;
	}
	
	public String getData(String columnName){
		String result = null;
		if(data.containsKey(columnName)){
			result = data.get(columnName);
		}
		return result;
	}
	
	public boolean isActive(){
		return data.get("Active").equals("true");
	}
	
	public void markActive(){
		data.put("Active","true");
	}
	
	public void markDeleted(){
		data.put("Active", "false");
	}
	
	public static String serializeString(String arg){
		
		if(arg==null)
			return "NULL";
		StringBuilder sb = new StringBuilder();
		sb.append("\"");
		for(int i=0;i<arg.length();i++){
			switch(arg.charAt(i)){
				case ',':
					sb.append('\\');
					sb.append(',');
					break;
				case '\n':
					sb.append("&nl;");
					break;
				case '\"':
					sb.append("\\\"");
					break;
				case ';':
					sb.append("\\;");
					break;
				case '\\':
					sb.append("\\\\");
					break;
				default:
					sb.append(arg.charAt(i));
			}
		}
		sb.append("\"");
		return sb.toString();	
	}
	
	public static String deserializeString(String arg){
		StringBuilder sb = new StringBuilder();
		if(arg.equals("NULL"))
			return null;
		int i=0;
		boolean opened = false;
		boolean escaped = false;
		arg = arg.replace("&nl;", "\n");
		while(i<arg.length()){
			if(!opened && arg.charAt(i) == '\"'){
				opened = true;
			}
			else if(!escaped && opened && arg.charAt(i) == '\"'){
				//closed = true;
				break;
			}
			else if(!escaped && arg.charAt(i)== '\\'){
				escaped = true;
			}
			else if(escaped){
				escaped = false;
				sb.append(arg.charAt(i));
			} else {
				sb.append(arg.charAt(i));
			}
			i++;
		}
		return sb.toString(); 
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		if(layout == null || layout.length == 0)
			return "";
		int i = 1;
		sb.append(Record.serializeString(data.get(layout[0])));
		for(i=1;i<layout.length;i++){
			sb.append(".,");
			sb.append(Record.serializeString(data.get(layout[i])));
		}
		return sb.toString();
	}
	
	public HashMap<String,String> getMap(){
		return this.data;
	}
	
}
