package org.bio_gene.wookie.connection;

import java.io.File;
import java.sql.ResultSet;

public class ImplCurlConnection implements Connection {

	public Boolean uploadFile(File file) {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean uploadFile(String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean uploadFile(File file, String graphURI) {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean uploadFile(String fileName, String graphURI) {
		// TODO Auto-generated method stub
		return null;
	}

	public void setDefaultUploadType(UploadType type) {
		// TODO Auto-generated method stub
		
	}

	public Boolean close() {
		// TODO Auto-generated method stub
		return null;
	}

	public ResultSet select(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean update(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	public ResultSet execute(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	public Boolean dropGraph(String graphURI) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void autoCommit(Boolean autoCommit) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beginTransaction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void endTransaction() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setUploadType(UploadType type) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDefaultGraph(String graph) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setConnection(java.sql.Connection con) {
		// TODO Auto-generated method stub
		
	}

}
