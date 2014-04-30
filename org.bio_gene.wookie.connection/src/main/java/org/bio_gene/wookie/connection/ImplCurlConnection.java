package org.bio_gene.wookie.connection;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bio_gene.wookie.connection.Connection.ModelUnionType;
import org.bio_gene.wookie.connection.Connection.UploadType;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

public class ImplCurlConnection implements Connection {

	private java.sql.Connection con;
	private Logger log;
	private Boolean autoCommit;
	private UploadType type;
	private ModelUnionType mut;
	private boolean transaction;
	
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void autoCommit(Boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	@Override
	public void beginTransaction() {
		this.transaction = true;	
	}

	@Override
	public void endTransaction() {
		if(commit()){
			this.transaction = false;
		}
	}

	private boolean commit() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setDefaultGraph(String graph) {
		LinkedList<String> graphs = new LinkedList<String>();
		graphs.add(graph);
		((SPARQLConnection) this.con).setDefaultGraphs(graphs);
	}

	@Override
	public void setConnection(java.sql.Connection con) {
		this.con = con;
	}

	@Override
	public void setModelUnionType(ModelUnionType mut) {
		this.mut = mut;
	}

}
