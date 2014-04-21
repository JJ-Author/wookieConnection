package org.bio_gene.wookie.tests;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URLConnection;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.activation.FileTypeMap;
import javax.activation.MimetypesFileTypeMap;

import org.apache.commons.lang3.SystemUtils;
import org.apache.jena.riot.Lang;
import org.bio_gene.wookie.utils.LogHandler;
import org.clapper.util.misc.MIMETypeUtil;
import org.junit.Test;

import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.sparql.engine.http.HttpContentType;


public class UtilityTests {
	
	@Test
	public void logTest(){

		Logger log = Logger.getLogger(this.getClass().getName());
		assertNotNull(log);
		log.setLevel(Level.FINEST);
		LogHandler.initLogFileHandler(log, "test");
		File file = new File("logs/test+.log.0");
		assertTrue(file.exists());
		TypeMapper tm = TypeMapper.getInstance();
		Lang l = Lang.guess("C:\\Users\\urFaust\\skypedatei\\Teichmann_Blank.ttl");
		log.info(l.getContentType().toString());
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
