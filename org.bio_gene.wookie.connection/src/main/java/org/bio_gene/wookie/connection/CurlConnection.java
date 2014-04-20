package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bio_gene.wookie.utils.CurlProcess;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;

public class CurlConnection extends CurlProcess implements Connection {

	private String endpoint;
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
	private Collection<String> dropGraphs;
	private UploadType type;
	
	public CurlConnection(String endpoint,String driver, String user,String password,String curlCommand,String curlDrop){
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
		this.endpoint = endpoint;
		this.driver = driver;
		this.user = user;
		this.password = password;
		this.curlCommand = curlCommand;
		this.curlDrop = curlDrop;
	}
	
	public void setDefaultGraphURI(String graphURI){
		if(graphURI == null){
			((SPARQLConnection) this.con).setDefaultGraphs(null);
		}
		LinkedList<String> graphs = new LinkedList<String>();
		graphs.add(graphURI);
		((SPARQLConnection) this.con).setDefaultGraphs(graphs);
	}
	
	private Boolean uploadFileIntern(File file, String graphURI){
		if(!file.exists()){
			try{
				throw new FileNotFoundException();
			}
			catch(FileNotFoundException e){
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
				return false;
			}
		}
		/*
		 * @TODO: curlCommand setzen
		 */
		String command=this.curlCommand;
		return this.process(command);
	}
	
	
	public Boolean uploadFile(File file) {
		return uploadFileIntern(file, ((SPARQLConnection)this.con).getDefaultGraphs().get(0));
	}

	public Boolean uploadFile(String fileName) {
		File f = new File(fileName);
		return uploadFile(f);
	}

	public Boolean uploadFile(File file, String graphURI) {
		return uploadFileIntern(file, graphURI);
	}

	public Boolean uploadFile(String fileName, String graphURI) {
		File f = new File(fileName);
		return uploadFile(f, graphURI);
	}

	public void setUploadType(UploadType type) {
		this.type = type;
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

	public ResultSet select(String query){
		try{
			Statement stm = this.con.createStatement();
			ResultSet rs = stm.executeQuery(query);
			stm.close();
			return rs;
		}
		catch(SQLException e){
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	}

	public Boolean update(String query) {
		try {
			Statement stm = this.con.createStatement();
			stm.executeUpdate(query);
			stm.close();
			return true;
		} catch (SQLException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return false;
		}
	}

	public ResultSet execute(String query) {
		try {
			Statement stm = this.con.createStatement();
			if(stm.execute(query)){
				return stm.getResultSet();
			}
			return null;
		} catch (SQLException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	}

	public Boolean dropGraph(String graphURI) {
		this.dropGraphs.add(graphURI);
		if(this.autoCommit){
			this.beginTransaction();
			Boolean ret = this.commit();
			this.endTransaction();
			if(ret){
				this.dropGraphs.clear();
			}
			return ret;
		}
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
			//Input TODO: curl change!
			String curl = "";
			if(this.transactionInput!= null){
				curl = this.curlCommand;
			}
			if(this.transactionDelete != null){
				curl += "\n "+this.curlDrop;
			}
			if(!this.dropGraphs.isEmpty()){
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
