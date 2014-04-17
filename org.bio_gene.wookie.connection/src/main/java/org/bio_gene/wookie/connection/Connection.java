package org.bio_gene.wookie.connection;

import java.io.File;
import java.sql.ResultSet;

public interface Connection  {
	
	public enum UploadType{
		POST, PUT, DELETE
	}
	
	public Boolean uploadFile(File file);
	public Boolean uploadFile(String fileName);
	public Boolean uploadFile(File file, String graphURI);
	public Boolean uploadFile(String fileName, String graphURI);
	
	public void autoCommit(Boolean autoCommit);
	public void beginTransaction();
	public void endTransaction();
	
	public void setDefaultUploadType(UploadType type);
	
	public Boolean close();
	
	public ResultSet select(String query);
	public Boolean update(String query);
	public ResultSet execute(String query);
	
	public Boolean dropGraph(String graphURI);
	
	
	
}
