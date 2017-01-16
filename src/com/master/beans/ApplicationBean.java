package com.master.beans;

import java.util.*;

public class ApplicationBean 
{
	private String appName;
	private String rowKey;
	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	private Map<String, List<String>> columnFamilies;
	
	private Map<String,Map<String,String>> columns;
	
	public Map<String,Map<String,String>> getColumns() {
		return columns;
	}

	public void setColumns(Map<String,Map<String,String>> columns) {
		this.columns = columns;
	}

	public ApplicationBean(String appName)
	{
		this.appName = appName;
		this.columnFamilies = new HashMap<String, List<String>>();
		this.columns = new HashMap<String, Map<String,String>>();
	}
	
	public String getAppName() 
	{
		return appName;
	}
	
	public void setAppName(String appName) 
	{
		this.appName = appName;
	}

	public Map<String, List<String>> getColumnFamilies() 
	{
		return columnFamilies;
	}

	public void setColumnFamilies(Map<String, List<String>> columnFamilies) 
	{
		this.columnFamilies = columnFamilies;
	}
	
	
}