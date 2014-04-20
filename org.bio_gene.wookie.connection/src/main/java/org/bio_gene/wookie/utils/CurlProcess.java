package org.bio_gene.wookie.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.SystemUtils;

public class CurlProcess {
	
	private Logger log;
	
	protected void setLogger(Logger log){
		this.log = log;
	}
	
	protected File setData(String data, String suffix){
		UUID gen = UUID.randomUUID();
		File script = new File(String.valueOf(gen.timestamp())+suffix);
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
			return null;
		}
		pw.write(data);
		pw.close();
		return script;
	}
	
	protected Boolean process(String command){
		File script;
		String suffix = ".sh";
		if(SystemUtils.IS_OS_WINDOWS){
			suffix = ".bat";
		}
		if((script= setData(command, suffix))==null){
			return false;
		}
		script.setExecutable(true);
		ProcessBuilder pb = new ProcessBuilder("."+File.separator+script.getName());
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
