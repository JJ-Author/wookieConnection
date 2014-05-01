package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.jena.riot.RDFLanguages;
import org.bio_gene.wookie.connection.Connection.ModelUnionType;
import org.bio_gene.wookie.connection.Connection.UploadType;
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.GraphHandler;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class ImplCurlConnection implements Connection {

	private java.sql.Connection con;
	private Logger log;
	private Boolean autoCommit;
	private UploadType type;
	private ModelUnionType mut;
	private boolean transaction;
	private String curlURL;
	private HashMap<String, String> props;
	private AuthType authType = AuthType.BASIC;
	private String user;
	private String pwd;
	private Graph transactionInput;
	private String queries="";
	
	public enum AuthType{
		NONE, BASIC, DIGEST
	}
	
	public void setAuthType(AuthType authType){
		this.authType = authType;
	}
	
	public void addProperty(String str1, String str2){
		this.props.put(str1, str2);
	}
	
	private Boolean writeData(String data){
		
		try {
			URL obj = new URL(this.curlURL);
			HttpURLConnection con = (HttpURLConnection) obj.openConnection();
			for(String key : props.keySet()){
				con.addRequestProperty(key, props.get(key));
			}
			con.setDoOutput(true);
			con.setRequestMethod(this.type.name());
			if(!this.authType.equals(AuthType.NONE)){
				String userpass = this.user + ":" + this.pwd;
				String basicAuth = this.authType.name().toLowerCase()+" " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes("UTF-8"));
				con.setRequestProperty ("Authorization", basicAuth);
			}
			OutputStreamWriter out = new OutputStreamWriter(con.getOutputStream());
			out.write(data);
			out.close();
			
			new InputStreamReader(con.getInputStream());
		} catch (IOException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return false;
		}
		return true;
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
			return writeData(update.replace(" ", "+"));
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

	
	public Boolean update(String query) {
		if(this.autoCommit){
			return writeData(query.replace(" ", "+"));
		}
		this.queries +=query.replace(" ", "+")+"\n";
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
		if(autoCommit){
			return this.writeData(query.replace(" ", "+"));
		}
		this.queries += query.replace(" ", "+")+"\n";
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
		//no need but i'm paranoid ;)
		if(this.transaction){
			return writeData(this.queries);
			
		}
		log.warning("No current Transaction!");
		
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
