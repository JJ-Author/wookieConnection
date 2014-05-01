package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.riot.RDFLanguages;
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.GraphHandler;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class ImplConnection implements Connection {

	private java.sql.Connection con;
	private Logger log;
	private UploadType type = UploadType.POST;
	private Boolean autoCommit = true;
	private ModelUnionType mut = ModelUnionType.add;
	private Graph transactionInput;
	private String queries = "";
	private boolean transaction;
	
	public ImplConnection(){
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
	}
	
	private void setLogger(Logger log) {
		this.log =log;
	}

	public Boolean uploadFile(File file) {
		return uploadFile(file, null);
	}

	public Boolean uploadFile(String fileName) {
		return uploadFile(new File(fileName));
	}

	public Boolean uploadFile(File file, String graphURI) {
		if(!file.exists()){
			try{
				throw new FileNotFoundException();
			}
			catch(FileNotFoundException e){
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
				return false;
			}
		}
		String absFile = file.getAbsolutePath()+File.separator+file.getName();
		String contentType = RDFLanguages.guessContentType(absFile).getContentType();
		
			Model input = ModelFactory.createModelForGraph(transactionInput);
			Model add = ModelFactory.createDefaultModel();
			try {
				add.read(new FileInputStream(file), null, FileExtensionToRDFContentTypeMapper.guessFileFormat(contentType));
				switch(this.mut){
				case add:
					input.add(add);
					break;
				case union:
					input.union(add);
					break;
				default:
					input.add(add);
				}
				this.transactionInput = input.getGraph();
			} catch (FileNotFoundException e) {
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
				return false;
			}
			if(!this.autoCommit){
				return null;
			}
			String update = "INSERT ";
			if(graphURI !=null){
				update+=" { GRAPH <"+graphURI+"> ";
			}
			else if(((SPARQLConnection) this.con).getDefaultGraphs().size()>0){
				update += " { GRAPH <"+((SPARQLConnection) this.con).getDefaultGraphs().get(0)+"> ";
			}
			update+=GraphHandler.GraphToSPARQLString(input.getGraph());
			if(graphURI !=null || ((SPARQLConnection) this.con).getDefaultGraphs().size()>0){
				update+=" }";
			}
			return this.update(update);
	}
	
	public Boolean uploadFile(String fileName, String graphURI) {
		return uploadFile(new File(fileName), graphURI);
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

	
	private Boolean updateIntern(String query){
		switch(this.type){
		case PUT:
			String pattern = "GRAPH <[^>]*>";
			Matcher m = Pattern.compile(pattern).matcher(query);
			while(m.find()){
				dropGraph(m.group().replace("GRAPH <", "").replace(">", ""));
			}
			break;
		case POST:
			break;
		default:
			return false;
		}
		try{
			Statement stm = this.con.createStatement();
			stm.executeUpdate(query);
			stm.close();
			this.queries ="";
			return true;
		} catch (SQLException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return false;
		}
	}
	
	
	public Boolean update(String query) {
		if(autoCommit){
			return updateIntern(query);
		}
		this.queries += query+="\n";
		return null;
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
			if(graphURI==null){
				log.warning("to be deleted Graph URI is null");
				return false;
			}
			String query = "DELETE { GRAPH <"+graphURI+"> }";
			return this.update(query);
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
		//no need but i'm paranoid ;)
				if(this.transaction){
					return updateIntern(this.queries);
					
				}
				log.warning("No current Transaction!");
				
				return false;
	}

	@Override
	public void setDefaultGraph(String graph) {
		if(graph == null){
			((SPARQLConnection) this.con).setDefaultGraphs(null);
		}
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
