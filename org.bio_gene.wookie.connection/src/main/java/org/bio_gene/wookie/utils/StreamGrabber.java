package org.bio_gene.wookie.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamGrabber extends Thread {

	private InputStream is;
	private Level lvl;
	private Logger log;
	
	public StreamGrabber(InputStream is, Level lvl, Logger log){
		this.is = is;
		this.lvl = lvl;
		this.log = log;
	}
	
	public void log() throws IOException{
		InputStreamReader reader = new InputStreamReader(is);
		BufferedReader br  = new BufferedReader(reader);
		String line=null;
		while((line=br.readLine())!=null){
			log.log(lvl, line);
		}
	}
	
	public void run(){
		try{
			log();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
}
