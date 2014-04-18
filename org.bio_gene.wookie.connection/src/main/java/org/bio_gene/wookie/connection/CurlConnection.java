package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bio_gene.wookie.utils.CurlProcess;
import org.bio_gene.wookie.utils.LogHandler;

import com.hp.hpl.jena.graph.Graph;

public class CurlConnection extends CurlProcess implements Connection {

	private String jdbcURL;
	private String driver;
	private String user;
	private String password;
	private String curlCommand;
	private String curlDrop;
	private java.sql.Connection con;
	private Logger log;
	private Boolean autoCommit = true;
	private Boolean transaction = false;
	private Graph transactionInput;
	private Graph transactionDelete;
	
	public CurlConnection(String jdbcURL,String driver,String user,String password,String curlCommand,String curlDrop){
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
		this.jdbcURL = jdbcURL;
		this.driver = driver;
		this.user = user;
		this.password = password;
		this.curlCommand = curlCommand;
		this.curlDrop = curlDrop;
	}
	
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
		try {
			this.con.close();
			return true;
		} catch (SQLException e) {
			log.severe("Couldn't close Connection: ");
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return false;
		}
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

	
	public void autoCommit(Boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	@Override
	public void beginTransaction() {
		this.transaction = true;		
	}

	private Boolean commit(){
		//no need but i'm paranoid ;)
		if(this.transaction){
			//Input
			String curl = "";
			if(this.transactionInput!= null){
				curl = this.curlCommand;
			}
			if(this.transactionDelete != null){
				curl += "\n "+this.curlDrop;
			}
			return process(curl);
		}
		log.warning("No current Transaction!");
		
		return false;
	}
	

	
	@Override
	public void endTransaction() {
		if(commit()){
			this.transaction = false;
		}
	}

	@Override
	public void setConnection(java.sql.Connection con) {
		this.con = con;
		
	}
	

}
