package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpOp;
import org.bio_gene.wookie.utils.CurlProcess;
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.GraphHandler;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.modify.UpdateProcessRemoteForm;
import com.hp.hpl.jena.sparql.modify.request.UpdateLoad;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.update.UpdateProcessor;
import com.hp.hpl.jena.update.UpdateRequest;

/**
 * Connection basierend auf Curl Prozessen (auch anderen Scripten).
 * 
 * @author Felix Conrads
 *
 */
@Deprecated
public class CurlConnection extends CurlProcess implements Connection {

	private int queryTimeout=180;

	private String curlCommand;
	private String curlDrop;
	private String curlURL;
	private String curlUpdate;
	private String mimeType;
	private String contentType;
	private java.sql.Connection con;
	private Logger log;
	private Boolean autoCommit = true;
	private Boolean transaction = false;
	private Graph transactionInput;
	private Graph transactionDelete;
	private Collection<String> dropGraphs = new HashSet<String>();
	private UploadType type =  UploadType.POST;
	private ModelUnionType mut = ModelUnionType.add;
	private String endpoint;
	private String user;
	private String pwd;
	private String updateEndpoint;

	private int numberOfTriples;

	
	/**
	 * Setzt ob die internen Graphen (bei autoCommit false) hinzugefügt oder vereinigt werden soll
	 * 
	 * @param mut ADD oder UNION
	 */
	public void setModelUnionType(ModelUnionType mut){
		this.mut = mut;
	}
	
	/**
	 * ERSTELLT KEINE CONNECTION!
	 * ConnectionFactory benutzen!
	 * @param endpoint
	 * @param user
	 * @param password
	 * @param curlCommand
	 * @param curlDrop
	 * @param curlURL
	 * @param curlUpdate
	 */
	public CurlConnection(String endpoint, String user,String password, String curlCommand,String curlDrop, 
			String curlURL, String curlUpdate, String updateEndpoint, int queryTimeout){
		this.queryTimeout = queryTimeout;
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
		this.curlCommand = curlCommand;
		this.curlDrop = curlDrop;
		this.curlURL = curlURL!=null? curlURL : endpoint;
		this.curlUpdate = curlUpdate;
		this.endpoint = "http://"+endpoint;
		this.updateEndpoint = "http://"+updateEndpoint;
		this.user = user;
		this.pwd = password;
		
	}
	
	/**
	 * Setzt den Standard MimeType (intern nicht genutzt)
	 * 
	 * @param mimeType 
	 */
	public void setMimeType(String mimeType){
		this.mimeType = mimeType;
	}
	
	/**
	 * Setzt den Standard ContentType (intern nicht genutzt)
	 * 
	 * @param contentType
	 */
	public void setContentType(String contentType){
		this.contentType = contentType;
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
	 * Setzt den zu benutzenen Default Graph (wird verwendet sofern keine GraphURI extra angegeben wird
	 * oder null ist
	 * 
	 * @param graph Name des Graph
	 */
	public void setDefaultGraph(String graph){
		if(graph == null){
			((SPARQLConnection) this.con).setDefaultGraphs(null);
		}
		LinkedList<String> graphs = new LinkedList<String>();
		graphs.add(graph);
		((SPARQLConnection) this.con).setDefaultGraphs(graphs);
	}
	
	
	private Long uploadFileIntern(File file, String graphURI){
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
		
		if(!this.autoCommit){
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
				return -1L;
			}
			return null;
		}
		//Is there a difference?? 
		String mimeType = contentType;
		String urlEncoded;
		try {
			urlEncoded = URLEncoder.encode(graphURI, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			urlEncoded=graphURI;
		}
		String command=this.curlCommand.replace("$GRAPH-URI", urlEncoded)
				.replace("$FILE", absFile)
				.replace("$CONTENT-TYPE", contentType)
				.replace("$MIME-TYPE", mimeType)
				.replace("$CURL-URL", this.curlURL);
		if(this.type.equals(UploadType.POST)){
			command = command.replace("$UPLOAD-TYPE", "-X POST");
		}
		else if(this.type.equals(UploadType.PUT)){
			command = command.replace("$UPLOAD-TYPE", " ");
		}
		long a = new Date().getTime();
		if(!this.process(command)){
			return -1L;
		}
		long b = new Date().getTime();
		return b-a;
	}
	
	
	/**
	 * Läd die angegebene Datei in den Default Graph sofern in curlCommand $GRAPH-URI gesetzt ist
	 * 
	 * @param file Das hochzuladene File
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Long uploadFile(File file) {
		return uploadFileIntern(file, ((SPARQLConnection)this.con).getDefaultGraphs().get(0));
	}

	/**
	 * Läd die angegebene Datei in den Default Graph sofern in curlCommand $GRAPH-URI gesetzt ist
	 * 
	 * @param fileName Name der Hochzuladenen Datei
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Long uploadFile(String fileName) {
		File f = new File(fileName);
		return uploadFile(f);
	}

	/**
	 * Läd die angegebene Datei in den angegebenen Graph sofern in curlCommand $GRAPH-URI gesetzt ist
	 * 
	 * @param file Das hochzuladene File
	 * @param graphURI Graph in den geladen werden soll
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Long uploadFile(File file, String graphURI) {
		return uploadFileIntern(file, graphURI);
	}

	/**
	 * Läd die angegebene Datei in den angegebenen Graph sofern in curlCommand $GRAPH-URI gesetzt ist
	 * 
	 * @param fileName Name der hochzuladenen Datei
	 * @param graphURI Graph in den geladen werden soll
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Long uploadFile(String fileName, String graphURI) {
		File f = new File(fileName);
		return uploadFile(f, graphURI);
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
			ResultSet rs = stm.executeQuery(query);
//			stm.close();
			return rs;
		}
		catch(SQLException e){
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	}

	
	/**
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird ein SPARQL-UPDATE Befehl ausgeführt
	 * (unabhängig von autoCommit und uploadType )
	 * 
	 * @param query Auszuführende Query
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long update(String query) {
		/*
		 *TODO  Update and execute must be written in Curl! Update must ask if autoCommit
		 */
		try {
			return ownUpdate(query);
		} catch (Exception e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return -1L;
		}
	}
	
	
	public long loadUpdate(String filename, String graphURI){
		HttpContext httpContext = new BasicHttpContext();
		CredentialsProvider provider = new BasicCredentialsProvider();
		provider.setCredentials(new AuthScope(AuthScope.ANY_HOST,
		    AuthScope.ANY_PORT), new UsernamePasswordCredentials(user, pwd));
		httpContext.setAttribute(ClientContext.CREDS_PROVIDER, provider);

//		GraphStore graphStore = GraphStoreFactory.create() ;
		UpdateRequest request = UpdateFactory.create();
		request.add(new UpdateLoad(filename, graphURI));
		UpdateProcessor processor = UpdateExecutionFactory
			    .createRemoteForm(request, updateEndpoint);
			((UpdateProcessRemoteForm)processor).setHttpContext(httpContext);
			long a = new Date().getTime();
			processor.execute();
			long b = new Date().getTime();
		return b-a;
	}
	

	private Long ownUpdate(String query){
		HttpContext httpContext = new BasicHttpContext();
		
		CredentialsProvider provider = new BasicCredentialsProvider();
		provider.setCredentials(new AuthScope(AuthScope.ANY_HOST,
		    AuthScope.ANY_PORT), new UsernamePasswordCredentials(user, pwd));
		httpContext.setAttribute(ClientContext.CREDS_PROVIDER, provider);

//		GraphStore graphStore = GraphStoreFactory.create() ;
		if(this.curlUpdate!=null && !this.curlUpdate.isEmpty()){
			try {
				long a = new Date().getTime();
				process(this.curlUpdate
					.replace("$USER", user)
					.replace("$PWD", pwd)
					.replace("$UPDATE", URLEncoder.encode(query, "UTF-8"))
					.replace("$CONTENT-TYPE", "application/x-www-form-urlencoded")
					.replace("$MIME-TYPE", "application/x-www-form-urlencoded")
					.replace("$CURL-URL", this.curlURL)
					.replace("$ENDPOINT", this.updateEndpoint));
				long b = new Date().getTime();
				return b-a;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1L;
			}
		}else{
		UpdateRequest request = UpdateFactory.create(query.replace("\n", " "));
		UpdateProcessor processor = UpdateExecutionFactory
			    .createRemoteForm(request, updateEndpoint);
			((UpdateProcessRemoteForm)processor).setHttpContext(httpContext);
			long a = new Date().getTime();
			processor.execute();
			long b = new Date().getTime();
			return b-a;
		}
//		String ep=updateEndpoint;
////		if (query != null && !query.equals("")) {
////            ep = endpoint.contains("?") ? endpoint + "&" + URLEncoder.encode(query): endpoint + "?" + URLEncoder.encode(query);
////     
//			HttpOp.execHttpPost(ep, "application/x-www-form-urlencoded", query.replace("\n", " ") , null, httpContext, null) ;

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
			stm.close();
			return null;
		} catch (SQLException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
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
//		this.dropGraphs.add(graphURI);
		if(this.autoCommit){
			//TODO ändern!!!
//			String curl="";
////			for(String graph : this.dropGraphs){
//				curl = this.curlDrop.replace("$GRAPH-URI", graphURI).replace("$CURL-URL", curlURL);
////			}
//			Boolean ret = process(curl);
			Long  ret = update("DROP SILENT GRAPH <"+graphURI+">");
//			if(ret){
//				this.dropGraphs.clear();
//			}
			return ret;
		}
		return null;
	}

	/**
	 * Setzt ob alle nicht autoCommit unabhängigen Befehle sofort ausgeführt werden sollen 
	 * oder erst bei endTransaction()
	 * (EXPERIMENTAL!!!!)
	 * 
	 * @param autoCommit Setzt autoCommit  
	 */
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

	private Boolean commit(){
		//no need but i'm paranoid ;)
		if(this.transaction){
			String curl = "";
			File file = new File(UUID.randomUUID().toString());
			try{
			Model m = ModelFactory.createModelForGraph(this.transactionInput);
			try {
				file.createNewFile();
				String fileFormat = FileExtensionToRDFContentTypeMapper.guessFileFormat(this.contentType);
				if(fileFormat.equals("PLAIN-TEXT")){
					fileFormat = "TURTLE";
				}
				m.write(new FileOutputStream(file), fileFormat);
			} catch (IOException e) {
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
				return false;
			}
			}catch(NullPointerException e){}
			String graphURI ="";
			try{
				graphURI = ((SPARQLConnection) this.con).getDefaultGraphs().get(0);
			}
			catch(Exception e){
			}
			if(this.transactionInput!= null){
				curl = this.curlCommand.replace("$GRAPH-URI", 
							graphURI)
						.replace("$FILE", file.getAbsolutePath()+File.separator+file.getName())
						.replace("$CONTENT-TYPE", this.contentType)
						.replace("$MIME-TYPE", this.mimeType)
						.replace("$UPLOAD-TYPE", this.type.toString())
						.replace("$CURL-URL", this.curlURL);
			}
			if(this.transactionDelete != null){
				String deleter =	"DELETE ";
				deleter += graphURI.equals("") ? "" : "{ GRAPH <"+graphURI+"> "; 
				deleter += GraphHandler.GraphToSPARQLString(transactionDelete);
				deleter += graphURI.equals("") ? "" : " }"; 
				curl += "\n "+this.curlUpdate.replace("$UPDATE", deleter);
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
	
	/**
	 * Beendet Transaktion und commitet geänderte Daten
	 * (EXPERIMENTAL!!!)
	 * 
	 */
	@Override
	public void endTransaction() {
		if(commit()){
			this.transaction = false;
		}
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

	@Override
	public String getEndpoint() {
		return endpoint;
	}

	@Override
	public void setEndpoint(String endpoint) {
		this.endpoint =endpoint;
	}

	@Override
	public String getUser() {
		return user;
	}

	@Override
	public void setUser(String user) {
		this.user =user;
	}


	@Override
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	@Override
	public void setUpdateEndpoint(String updateEndpoint) {
		this.updateEndpoint = updateEndpoint;
	}
	
	
	

	@Override
	public void setTriplesToUpload(long count) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long getTriplesToUpload() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Long deleteFile(File file, String graphURI) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long deleteFile(String file, String graphURI) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long selectTime(String query, int queryTimeout) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long selectTime(String query) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean isClosed() {
		// TODO Auto-generated method stub
		return null;
	}

}
