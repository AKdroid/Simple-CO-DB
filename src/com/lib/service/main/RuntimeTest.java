package com.lib.service.main;

import java.io.File;
import java.io.IOException;

public class RuntimeTest {
	
	public static void main(String args[]){
		File f = new File("scripts");
		try {
			String path = "/home/akhil/test/Address";
			String[] cmd = {
			        "/bin/bash",
			        "-c",
			        "python"+ f.getAbsolutePath()+"/rowCount.py '" + path + "'"
			    };
			Runtime.getRuntime().exec(cmd);
			
		} catch (IOException e) {
	
			e.printStackTrace();
		}
		
		
	}

}
