package com.lib.ds.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;

public class HashUtil {

	public static int getHash(String key, int index, int modulus){
		int hashval = 0;
		String digestedString = "";
		switch(index){
		case 1:
			digestedString = encryptPassword("SHA-1",key);
			break;
		case 2:
			digestedString = encryptPassword("SHA-256",key);
			break;
		case 3:
			digestedString = encryptPassword("SHA-384",key);
			break;
		case 4:
			digestedString = encryptPassword("SHA-512",key);
			break;
		default:
			return -1;
		}
		hashval = hash(digestedString)%modulus;
		return hashval;
	}
	
	private static String encryptPassword(String algoName, String password)
	{
	    String enPassword = "";
	    try
	    {
	        MessageDigest crypt = MessageDigest.getInstance(algoName);
	        crypt.reset();
	        crypt.update(password.getBytes("UTF-8"));
	        enPassword = byteToHex(crypt.digest());
	    }
	    catch(NoSuchAlgorithmException e)
	    {
	        e.printStackTrace();
	    }
	    catch(UnsupportedEncodingException e)
	    {
	        e.printStackTrace();
	    }
	    return enPassword;
	}

	private static String byteToHex(final byte[] hash)
	{
	    Formatter formatter = new Formatter();
	    for (byte b : hash)
	    {
	        formatter.format("%02x", b);
	    }
	    String result = formatter.toString();
	    formatter.close();
	    return result;
	}
	
	public static int hash(String encryptedKey)
	{
		int hash = 7;
		for (int i = 0; i < encryptedKey.length(); i++) {
			hash = hash*31 + encryptedKey.charAt(i);
		}
		if(hash < 0) hash = hash*-1;
		return hash;
	}
	
	public static int[] sampleWithoutReplacement(int modulus, int[] hashes){
		ArrayList<Integer> list = new ArrayList<Integer>(modulus);
		int[] sampledHashes = new int[hashes.length];
		for(int i=0;i<modulus;i++)
			list.add(i);
		for(int i=0;i<hashes.length;i++){
			sampledHashes[i] = list.get(hashes[i]%(modulus-i));
			list.remove(hashes[i]);
		}
		return sampledHashes;
	}
	
}
