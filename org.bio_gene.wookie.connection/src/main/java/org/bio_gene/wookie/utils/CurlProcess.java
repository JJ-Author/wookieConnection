package org.bio_gene.wookie.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CurlProcess {
	
	private Logger log;
	
	protected void setLogger(Logger log){
		this.log = log;
	}
	
	protected Boolean process(String command){
		UUID gen = UUID.randomUUID();
		File script = new File(String.valueOf(gen.timestamp()));
		while(script.exists()){
			//try to wait  a short random time 
			try {
				wait((int)(Math.random()*100));
			} catch (InterruptedException e) {
				LogHandler.writeStackTrace(log, e, Level.WARNING);
			}
			gen = UUID.randomUUID();
			script = new File(String.valueOf(gen.timestamp()));
		}
		PrintWriter pw = null;
		try {
			script.createNewFile();
			pw = new PrintWriter(script);
		} catch (IOException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return false;
		}
		pw.write(command);
		pw.close();
		
		script.setExecutable(true);
		ProcessBuilder pb = new ProcessBuilder("./"+script.getName());
		pb.directory(new File(script.getAbsolutePath().replace(
				script.getName(), File.separator)));
		try {
			pb.start().waitFor();
		} catch (InterruptedException | IOException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return false;
		}
		script.delete();
		return true;
	}
}
