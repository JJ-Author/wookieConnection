package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.riot.RDFLanguages;
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.FileHandler;
import org.bio_gene.wookie.utils.GraphHandler;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.modify.UpdateProcessRemoteForm;
import com.hp.hpl.jena.sparql.modify.request.UpdateLoad;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

/**
 * Connection rein basierend auf dem SPARQL Endpoint des Triplestores
 * 
 * @author Felix Conrads
 *
 */
public class ImplConnection implements Connection {

	private java.sql.Connection con;
	private int queryTimeout=180;
	private Logger log;
	private String endpoint;
	private String updateEndpoint;
	private UploadType type = UploadType.POST;
	private Boolean autoCommit = true;
	private ModelUnionType mut = ModelUnionType.add;
	private Graph transactionInput;
	private String queries = "";
	private boolean transaction;
	private String user;
	private String pwd;
	private long numberOfTriples;
	
	/**
	 * ERSTELLT KEINE CONNECTION!
	 * ConnectionFactory benutzen!
	 */
	public ImplConnection(int queryTimeout){
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
		this.queryTimeout = queryTimeout;
	}
	
	private void setLogger(Logger log) {
		this.log =log;
	}

	/**
	 * Läd die angegebene Datei in den Default Graph
	 * 
	 * @param file Das hochzuladene File
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Long uploadFile(File file) {
		return uploadFile(file, null);
	}

	/**
	 * Läd die angegebene Datei in den Default Graph
	 * 
	 * @param fileName Name der Hochzuladenen Datei
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Long uploadFile(String fileName) {
		return uploadFile(new File(fileName));
	}
	
	public Long uploadFile(File file, String graphURI){
		if(!file.exists()){
				LogHandler.writeStackTrace(log, new FileNotFoundException(), Level.SEVERE);
				return -1L;
		}
		Long ret = 0L;
		Boolean isFile=false;
		if(numberOfTriples<1){
			numberOfTriples =FileHandler.getLineCount(file);
			isFile=true;
		}
		try {
			for(File f : FileHandler.splitTempFile(file, numberOfTriples)){
				Long retTmp = uploadFileIntern(f, graphURI);
				if(retTmp==-1){
					log.severe("Couldn't upload part: "+f.getName());
				}else{
					ret+=retTmp;
				}
				if(!isFile)
					f.delete();
			}
		} catch (IOException e) {
			log.severe("Couldn't upload file(s) due to: ");
			ret =-1L;
		}
		return ret;
	}
	

	/**
	 * Läd die angegebene Datei in den angegebenen Graph
	 * 
	 * @param file Das hochzuladene File
	 * @param graphURI Graph in den geladen werden soll
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	private Long uploadFileIntern(File file, String graphURI) {
		if(!file.exists()){
			try{
				throw new FileNotFoundException();
			}
			catch(FileNotFoundException e){
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
				return -1L;
			}
		}
		
		String absFile = file.getAbsolutePath();
		String contentType = RDFLanguages.guessContentType(absFile).getContentType();
		Model input = null;
			if(this.transactionInput!= null){ 
				input = ModelFactory.createModelForGraph(transactionInput);
			}
			else{
				input = ModelFactory.createDefaultModel();
			}
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
				return -1L;
			}
			if(!this.autoCommit){
				return null;
			}
			String update = "INSERT DATA ";
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
			Long ret =  this.update(update);
			this.transactionInput = null;
			return ret;
	}
	
	/**
	 * Läd die angegebene Datei in den angegebenen Graph
	 * 
	 * @param fileName Name der hochzuladenen Datei
	 * @param graphURI Graph in den geladen werden soll
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Long uploadFile(String fileName, String graphURI) {
		return uploadFile(new File(fileName), graphURI);
	}

	/**
	 * Setzt ob beim Upload eines Files PUT oder POST benutzt werden soll
	 * 
	 * @param type PUT oder POST
	 */
	public void setUploadType(UploadType type) {
		this.type = type;
		
	}

	/**
	 * Schließt die interne java.sql.Connection
	 * 
	 * @return true wenn erfolgreich, andernfalls false
	 */
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
		return select(query, this.queryTimeout);
	}
	
	/**
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird eine Select Anfrage gestellt
	 * (unabhängig von autoCommit)
	 * 
	 * @param query Auszuführende Query
	 * @return ResultSet mit Ergebnissen der Query, bei Fehler null
	 */
	public ResultSet select(String query, int queryTimeout){
		try{
			Statement stm = this.con.createStatement();
			stm.setQueryTimeout(queryTimeout);
			ResultSet rs=null;
			rs = stm.executeQuery(query);
//			stm.close();
			return rs;
		}
		catch(SQLException e){
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	}

	
	private Long updateIntern(String query, Boolean drop){
		long ret=-1;
		if(!drop){
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
			return ret;
		}
		}
		try{
//			con.setAutoCommit(true);
//			con.setTransactionIsolation(8);
//			Statement stm = this.con.createStatement();
//			int updated = stm.executeUpdate(query);
			
			ret = ownUpdate(query);
			
//			stm.close();
//			if(updated==0){
//				log.warning("Update "+query+" doesn't succeeded");
//			}
			this.queries ="";
			return ret;
		} catch (Exception e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return ret;
		}
	}
	
	private long ownUpdate(String query){
		HttpContext httpContext = new BasicHttpContext();
		if(user!=null && pwd!=null){
			CredentialsProvider provider = new BasicCredentialsProvider();
			
			provider.setCredentials(new AuthScope(AuthScope.ANY_HOST,
					AuthScope.ANY_PORT), new UsernamePasswordCredentials(user, pwd));
			httpContext.setAttribute(ClientContext.CREDS_PROVIDER, provider);
		}
//		GraphStore graphStore = GraphStoreFactory.create() ;
		UpdateRequest request = UpdateFactory.create(query, Syntax.syntaxSPARQL_11);
		UpdateProcessor processor = UpdateExecutionFactory
			    .createRemoteForm(request, updateEndpoint);
			((UpdateProcessRemoteForm)processor).setHttpContext(httpContext);
			Long a = new Date().getTime();
			processor.execute();
		Long b = new Date().getTime();	
		return b-a;
	}
	
	public long loadUpdate(String filename, String graphURI){
		HttpContext httpContext = new BasicHttpContext();
		if(user!=null && pwd!=null){
			CredentialsProvider provider = new BasicCredentialsProvider();
			
			provider.setCredentials(new AuthScope(AuthScope.ANY_HOST,
					AuthScope.ANY_PORT), new UsernamePasswordCredentials(user, pwd));
			httpContext.setAttribute(ClientContext.CREDS_PROVIDER, provider);
		}

//		GraphStore graphStore = GraphStoreFactory.create() ;
		UpdateRequest request = UpdateFactory.create();
		request.add(new UpdateLoad(filename, graphURI));
		UpdateProcessor processor = UpdateExecutionFactory
			    .createRemoteForm(request, updateEndpoint);
			((UpdateProcessRemoteForm)processor).setHttpContext(httpContext);
		Long a = new Date().getTime();
			processor.execute();
		Long b = new Date().getTime();	
		return b-a;
	}
	
	/**
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird ein SPARQL-UPDATE Befehl ausgeführt
	 * 
	 * @param query Auszuführende Query
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long update(String query) {
		if(autoCommit){
			return updateIntern(query, false);
		}
		this.queries += query+="\n";
		return null;
	}
	
	public ResultSet execute(String query) {
		return execute(query, this.queryTimeout);
	}

	/**
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird ein SPARQL Befehl ausgeführt
	 * (unabhängig von autoCommit und uploadType )
	 * 
	 * @param query Auszuführende Query
	 * @return ResultSet falls Select anfrage, andernfalls null (ebenso bei Fehler)
	 */
	public ResultSet execute(String query, int queryTimeout) {
		try {
			Statement stm = this.con.createStatement();
			stm.setQueryTimeout(queryTimeout);
			if(stm.execute(query)){
				return stm.getResultSet();
			}
			return null;
		} catch (SQLException e) {
//			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	}

	/**
	 * Löscht den angegeben Graphen im TripleStore
	 * 
	 * @param graphURI
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long dropGraph(String graphURI) {
			if(graphURI==null){
				log.warning("to be deleted Graph URI is null");
				return -1L;
			}
			String query = "DROP SILENT GRAPH <"+graphURI+">";
			if(autoCommit){
				return updateIntern(query, true);
			}
			this.queries += query+="\n";
			return null;
	}

	/**
	 * Setzt ob alle nicht autoCommit unabhängigen Befehle sofort ausgeführt werden sollen 
	 * oder erst bei endTransaction()
	 * (EXPERIMENTAL!!!!)
	 * 
	 * @param autoCommit Setzt autoCommit  
	 */
	@Override
	public void autoCommit(Boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	/**
	 * Beginnt die Transaction wenn autoCommit off
	 * (EXPERIMENTAL!!!!)
	 */
	@Override
	public void beginTransaction() {
		this.transaction = true;	
	}

	/**
	 * Beendet Transaktion und commitet geänderte Daten
	 * (EXPERIMENTAL!!!)
	 * 
	 */
	@Override
	public void endTransaction() {
		if(commit()!=-1){
			this.transaction = false;
		}
	}

	private Long commit() {
		//no need but i'm paranoid ;)
				if(this.transaction){
					return updateIntern(this.queries, false);
					
				}
				log.warning("No current Transaction!");
				
				return -1L;
	}

	/**
	 * Setzt den zu benutzenen Default Graph (wird verwendet sofern keine GraphURI extra angegeben wird
	 * oder null ist
	 * 
	 * @param graph Name des Graph
	 */
	@Override
	public void setDefaultGraph(String graph) {
		if(graph == null){
			((SPARQLConnection) this.con).setDefaultGraphs(null);
		}
		LinkedList<String> graphs = new LinkedList<String>();
		graphs.add(graph);
		((SPARQLConnection) this.con).setDefaultGraphs(graphs);
	}

	/**
	 * Setzt die interne Connection
	 * 
	 * @param con java.sql.Connection zum Triplestore
	 */
	@Override
	public void setConnection(java.sql.Connection con) {
		this.con = con;
	}

	/**
	 * Setzt ob die internen Graphen (bei autoCommit false) hinzugefügt oder vereinigt werden soll
	 * 
	 * @param mut ADD oder UNION
	 */
	@Override
	public void setModelUnionType(ModelUnionType mut) {
		this.mut = mut;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public void setUpdateEndpoint(String endpoint) {
		this.updateEndpoint = endpoint;
	}

	
	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}


	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	@Override
	public void setTriplesToUpload(long count) {
		this.numberOfTriples = count;
	}

	@Override
	public long getTriplesToUpload() {
		return this.numberOfTriples;
	}
	
	

}
