package org.bio_gene.wookie.tests;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bio_gene.wookie.utils.LogHandler;
import org.junit.Test;


public class UtilityTests {
	
	@Test
	public void logTest(){
		Logger log = Logger.getLogger(this.getClass().getName());
		assertNotNull(log);
		log.setLevel(Level.FINEST);
		LogHandler.initLogFileHandler(log, "test");
		File file = new File("logs/test+.log.0");
		assertTrue(file.exists());
		log.warning("test2");
		log.info("Test");
		log.finest("test2");
		try{
			throw new NullPointerException();
		}
		catch(Exception e){
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
		}
		Long fileLength = file.length();
		assertTrue(fileLength > 0L);
		log.warning("test2");
	}
	
	

}
