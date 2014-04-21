package org.bio_gene.wookie.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Easy Log Handling
 *
 * @author Felix Conrads
 */
public class LogHandler {

	
	/**
	 * tries to add a FileHandler to the Logger with the given Name
	 * otherwise it will log the exception
	 * 
	 * @param log
	 * @param logName
	 */
	public static void initLogFileHandler(Logger log, String logName){
		try {
			//Ordner log gegebenfalls erstellen
			File file = new File("./logs");
			file.mkdirs();
			//Datei ./log/Connection.log mit Größe der max. Value eines Integer, max. 3 Dateien und append
			FileHandler fh = new FileHandler("./logs/"+logName+"+.log", Integer.MAX_VALUE, 3, true );
			ConsoleHandler ch = new ConsoleHandler();
			LogFormatter formatter = new LogFormatter();
			ch.setFormatter(formatter);
			fh.setFormatter(formatter);
			//Logger schreibt somit in die Datei
			log.addHandler(fh);
			log.addHandler(ch);
			log.setUseParentHandlers(false);
		} catch (SecurityException | IOException e) {
			//Logger schreibt StackTrace der Exception in die Konsole/Log-File
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			log.warning(sw.toString());
		}
	}

	
	/**
	 * Writes the StackTrace into the Logger
	 * 
	 * @param log
	 * @param e
	 * @param lvl
	 */
	public static void writeStackTrace(Logger log, Exception e, Level lvl){
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		log.log(lvl, sw.toString());
	}
	
}
