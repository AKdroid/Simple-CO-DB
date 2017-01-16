package com.lib.ds.utils;

import java.io.File;
import java.io.FileInputStream;
import java.util.Enumeration;
import java.util.Properties;

public class Configurations 
{
	public static String propertyFileName = "config.properties";
	
	public static String ROOT_DIRECTORY;
	public static int CLIENT_LISTEN_PORT;
	public static int INTERNAL_LISTEN_PORT;
	public static int NUMBER_OF_CLUSTERS;
	public static String ZOOKEEPER_HOST;
	public static int ZOOKEEPER_PORT;
	public static String CONNECTION_STRING;
	
	public Configurations() throws Exception 
	{
		try
		{
			Properties properties = new Properties();
			properties.load(new FileInputStream(getConfigFolderPath()+ propertyFileName));
			Enumeration<Object> bundleKeys = properties.keys();
	
			while (bundleKeys.hasMoreElements()) 
			{
				String key = bundleKeys.nextElement().toString();
				String value = properties.getProperty(key);
	
				if (key.equals("ROOT_DIRECTORY")) 
				{
					if(!value.endsWith("/"))
						value += "/";
					Configurations.ROOT_DIRECTORY = value;
				}
				else if (key.equals("CLIENT_LISTEN_PORT")) 
				{
					Configurations.CLIENT_LISTEN_PORT = Integer.parseInt(value);
				}
				else if (key.equals("INTERNAL_LISTEN_PORT")) 
				{
					Configurations.INTERNAL_LISTEN_PORT = Integer.parseInt(value);
				}
				else if (key.equals("NUMBER_OF_CLUSTERS")) 
				{
					Configurations.NUMBER_OF_CLUSTERS = Integer.parseInt(value);
				} 
				else if (key.equals("ZOOKEEPER_HOST"))
				{
					Configurations.ZOOKEEPER_HOST = value;
				}
				else if (key.equals("ZOOKEEPER_PORT"))
				{
					Configurations.ZOOKEEPER_PORT = Integer.parseInt(value);
				}
				else if (key.equals("CONNECTION_STRING"))
				{
					Configurations.CONNECTION_STRING = value;
				}
			}
		}
		catch (Exception e) 
		{
			System.out.println("Failed to load property file.");
			throw e;
		}
	}
	
	public String getConfigFolderPath() throws Exception 
	{
		String configFolderPath = null;
		File configFolder = new File("config");
		try 
		{
			configFolderPath = configFolder.getAbsolutePath()+"/";
			System.out.println(configFolderPath);
		}
		catch (Exception e) 
		{
			throw e;
		}

		return configFolderPath;
	}
}