package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.ArrayUtils;
import org.bio_gene.wookie.utils.CurlProcess;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;

public class CurlConnection extends CurlProcess implements Connection {

	private String endpoint;
	private String user;
	private String password;
	private String curlCommand;
	private String curlDrop;
	private String curlURL;
	private String mimeType;
	private String contentType;
	private java.sql.Connection con;
	private Logger log;
	private Boolean autoCommit = true;
	private Boolean transaction = false;
	private Graph transactionInput;
	private Graph transactionDelete;
	private Collection<String> dropGraphs;
	private UploadType type;
	
	public CurlConnection(String endpoint, String user,String password, String curlCommand,String curlDrop, String curlURL){
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
		this.endpoint = endpoint;
		this.user = user;
		this.password = password;
		this.curlCommand = curlCommand;
		this.curlDrop = curlDrop;
		this.curlURL = curlURL!=null? curlURL : endpoint;
		
	}
	
	public void setMimeType(String mimeType){
		this.mimeType = mimeType;
	}
	
	public void setContentType(String contentType){
		this.contentType = contentType;
	}

	public void setUploadType(UploadType type) {
		this.type = type;
	}
	
	public void setDefaultGraph(String graph){
		if(graph == null){
			((SPARQLConnection) this.con).setDefaultGraphs(null);
		}
		LinkedList<String> graphs = new LinkedList<String>();
		graphs.add(graph);
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
		String command=this.curlCommand.replace("$GRAPH_URI", graphURI)
				.replace("$FILE", file.getAbsolutePath()+File.separator+file.getName())
				.replace("$CONTENT-TYPE", this.contentType)
				.replace("$MIME-TYPE", this.mimeType)
				.replace("$UPLOAD-TYPE", this.type.toString());
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
			//Put the input graph into a File
			File file = new File(UUID.randomUUID().toString());
			try {
				file.createNewFile();
			} catch (IOException e) {
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
				return false;
			}
			if(this.transactionInput!= null){
				curl = this.curlCommand.replace("$GRAPH_URI", 
							((SPARQLConnection) this.con).getDefaultGraphs().get(0))
						.replace("$FILE", file.getAbsolutePath()+File.separator+file.getName())
						.replace("$CONTENT-TYPE", this.contentType)
						.replace("$MIME-TYPE", this.mimeType)
						.replace("$UPLOAD-TYPE", this.type.toString());
			}
			if(this.transactionDelete != null){
				curl += "\n "+this.curlDrop;
			}
			if(!this.dropGraphs.isEmpty()){
				for(String graph : this.dropGraphs){
					curl += "\n "+this.curlDrop.replace("$GRAPH-URI", graph);
				}
			}
			Boolean ret = process(curl);
			file.delete();
			return ret;
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
