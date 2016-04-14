package org.bio_gene.wookie.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.jena.riot.RDFLanguages;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;

public class FileHandler {
	
	private static Logger log = Logger.getGlobal();
	/**
	 * Gets the line count of a given file
	 *
	 * @param fileName the file name
	 * @return the line count
	 */
	public static long getLineCount(String fileName){
		return getLineCount(new File(fileName));
	}
	
	/**
	 * Gets the line count of a given file
	 *
	 * @param file the file
	 * @return the line count
	 */
	public static long getLineCount(File file){
		long lines=0;
		FileInputStream fis = null;
		BufferedReader br= null;
		try {
			if(!file.exists())
				file.createNewFile();
			fis = new FileInputStream(file);
			br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
			String line ="";
			while((line=br.readLine()) != null){
				if(!line.isEmpty() && !line.equals("\n")){
					lines++;
				}
			}
		} catch (IOException e) {
			LogHandler.writeStackTrace(log, e, Level.WARNING);
			lines= -1;
		} finally{
			if(br!=null){
				try {
					if(br!=null)		
						br.close();
				} catch (IOException e) {
					LogHandler.writeStackTrace(log, e, Level.WARNING);
				}
				try {
					if(fis!=null)
						fis.close();
				} catch (IOException e) {
					LogHandler.writeStackTrace(log, e, Level.WARNING);
				}
			}
		}
		return lines;
	}
	
	public static File[] splitTempFile(String fileName,long numberOfTriples) throws IOException{
		return splitTempFile(new File(fileName), numberOfTriples);
	}
	
	public static File[] splitTempFile(File file, long numberOfTriples) throws IOException{
		double divide = (FileHandler.getLineCount(file)*1.0)/numberOfTriples;
		File[] ret = new File[(int) Math.ceil(divide)];
		if(divide==1.0){
			ret[0] = file;
			return ret;
		}
		Model m = ModelFactory.createDefaultModel();
		String absFile = file.getAbsolutePath();
		String contentType = RDFLanguages.guessContentType(absFile).getContentType();
		FileInputStream input = new FileInputStream(file);
		m.read(input, null, FileExtensionToRDFContentTypeMapper.guessFileFormat(contentType));
		StmtIterator stmtIt = m.listStatements();
		for(int i=0; i<divide;i++){
			File tmp = File.createTempFile(file.getName()+i, ".nt");
			PrintWriter pw = new PrintWriter(tmp);
			
			Model tmpModel = ModelFactory.createDefaultModel();
			
			for(int j=0; j<numberOfTriples&&stmtIt.hasNext();j++){
				tmpModel.add(stmtIt.next());
			}
			tmpModel.write(pw, "N-TRIPLE");
			pw.close();
			ret[i] =tmp ;
		}
		input.close();
		return ret;
	}
}
