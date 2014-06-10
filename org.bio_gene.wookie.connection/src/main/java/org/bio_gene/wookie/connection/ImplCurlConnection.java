package org.bio_gene.wookie.connection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.auth.DigestScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.jena.riot.RDFLanguages;
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.GraphHandler;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Connection mit eigener interner Curl Connection
 * @deprecated  use {@link ImplConnection}
 * 
 * @author Felix Conrads
 *
 */
public class ImplCurlConnection implements Connection {

	private java.sql.Connection con;
	private Logger log;
	private Boolean autoCommit = true;
	private UploadType type = UploadType.POST;
	private ModelUnionType mut  = ModelUnionType.add;
	private boolean transaction;
	private String curlURL;
	private HashMap<String, String> props;
	private AuthType authType = AuthType.BASIC;
	private String user;
	private String pwd;
	private Graph transactionInput;
	private String queries="";
	
	/**
	 * Authentifizierungstypen für Curl Prozesse
	 * 
	 * @author Felix Conrads
	 *
	 */
	public enum AuthType{
		NONE, BASIC, DIGEST
	}
	
	/**
	 * ERSTELLT KEINE CONNECTION!
	 * ConnectionFactory benutzen!
	 * 
	 * @param authType
	 * @param props
	 */
	public ImplCurlConnection(String curlURL, String user, String pwd, String authType, HashMap<String, String> props) {
		this.curlURL = curlURL;
		this.user=user;
		this.pwd=pwd;
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
		if(props!=null){
			this.props = props;
		}
		else{
			this.props = new HashMap<String, String>();
		}
		if(authType != null){
			this.authType = AuthType.valueOf(authType);
		}
	}

	private void setLogger(Logger log) {
		this.log = log;
	}

	/**
	 * Setzt den zu benutzenden Authentifiezierungstypen
	 * 
	 * @param authType
	 */
	public void setAuthType(AuthType authType){
		this.authType = authType;
	}
	
	/**
	 * Fügt eine Curl Property hinzu 
	 * 
	 * @param str1 name der Property
	 * @param str2 Wert der Property
	 */
	public void addProperty(String str1, String str2){
		this.props.put(str1, str2);
	}
	
	private Boolean writeData(String data){
		
		try {
			URL url = new URL(this.curlURL);
			String auth = this.authType.name().toLowerCase();
			char[] stringArray = auth.trim().toCharArray();
			stringArray[0] = Character.toUpperCase(stringArray[0]);
	 		auth = new String(stringArray);

			
			HttpUriRequest req = null;
			StringEntity strEntity = new StringEntity(data.replace(" ", "+"), "UTF-8");
			if(this.type.equals(UploadType.POST)){
				HttpPost post = new HttpPost(this.curlURL);
				post.setEntity(strEntity);
				req = post;
				
			}
			else if(this.type.equals(UploadType.PUT)){
				HttpPut put = new HttpPut(this.curlURL);
				put.setEntity(strEntity);
				req = put;
			
			}
			req.setHeader("Content-Length", String.valueOf(strEntity.getContentLength()));
			req.setHeader("Accept", "text/html");
				HttpParams basicParams = new BasicHttpParams();
				for(String key: this.props.keySet()){
					basicParams.setParameter(key, props.get(key));
				}
				req.setParams(basicParams);
			
			req.setHeader("Content-type", "text/plain");
			req.setHeader("verbose", "true");
			Header header = null;
			UsernamePasswordCredentials cred = null;
			if(this.authType.equals(AuthType.BASIC)){
				cred = new UsernamePasswordCredentials(user, pwd);
				header = new BasicScheme().authenticate(cred, req);
				req.addHeader(header);
			}
			else if(this.authType.equals(AuthType.DIGEST)){
				cred = new UsernamePasswordCredentials(user, pwd);
				DigestScheme digest = new DigestScheme();
				digest.overrideParamter("nonce", "123456789");
				digest.overrideParamter("realm", this.authType.name().toLowerCase()); 
				header = digest.authenticate(cred, req);
				req.addHeader(header);
			}
			DefaultHttpClient client = new DefaultHttpClient();
	 		if(!this.authType.equals(AuthType.NONE)){
	 			CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
	 			credentialsProvider.setCredentials(new AuthScope(url.getHost(), url.getPort(), this.authType.name().toLowerCase()),cred);
	 			client.getCredentialsProvider().setCredentials(new AuthScope(url.getHost(), url.getPort(), this.authType.name().toLowerCase()), cred);

//	 					HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();
	 		}
	       
			HttpResponse res = client.execute(req);
			this.log.info(res.getStatusLine().toString());
			
		} catch (IOException | AuthenticationException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return false;
		}
		return true;
	}
	
	/**
	 * Läd die angegebene Datei in den Default Graph
	 * 
	 * @param file Das hochzuladene File
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Boolean uploadFile(File file) {
		return uploadFile(file, null);
	}
	/**
	 * Läd die angegebene Datei in den Default Graph
	 * 
	 * @param fileName Name der Hochzuladenen Datei
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Boolean uploadFile(String fileName) {
		return uploadFile(new File(fileName));
	}
	/**
	 * Läd die angegebene Datei in den angegebenen Graph
	 * 
	 * @param file Das hochzuladene File
	 * @param graphURI Graph in den geladen werden soll
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
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
	/**
	 * Läd die angegebene Datei in den angegebenen Graph
	 * 
	 * @param fileName Name der hochzuladenen Datei
	 * @param graphURI Graph in den geladen werden soll
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Boolean uploadFile(String fileName, String graphURI) {
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
	/**
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird eine Select Anfrage gestellt
	 * (unabhängig von autoCommit)
	 * 
	 * @param query Auszuführende Query
	 * @return ResultSet mit Ergebnissen der Query, bei Fehler null
	 */
	public ResultSet select(String query){
		try{
			Statement stm = this.con.createStatement();
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
	 * Basierend auf dem internen Curl Prozess
	 * wird ein SPARQL-UPDATE Befehl ausgeführt
	 * 
	 * @param query Auszuführende Query
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Boolean update(String query) {
		if(this.autoCommit){
			return writeData(query.replace(" ", "+"));
		}
		this.queries +=query.replace(" ", "+")+"\n";
		return null;
	}

	/**
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird ein SPARQL Befehl ausgeführt
	 * (unabhängig von autoCommit und uploadType )
	 * 
	 * @param query Auszuführende Query
	 * @return ResultSet falls Select anfrage, andernfalls null (ebenso bei Fehler)
	 */
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

	
	/**
	 * Löscht den angegeben Graphen im TripleStore
	 * 
	 * @param graphURI
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Boolean dropGraph(String graphURI) {
		if(graphURI==null){
			log.warning("to be deleted Graph URI is null");
			return false;
		}
		String query = "DROP SILENT GRAPH <"+graphURI+">";
		if(autoCommit){
			return this.writeData(query.replace(" ", "+"));
		}
		this.queries += query.replace(" ", "+")+"\n";
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

	/**
	 * Setzt den zu benutzenen Default Graph (wird verwendet sofern keine GraphURI extra angegeben wird
	 * oder null ist
	 * 
	 * @param graph Name des Graph
	 */
	@Override
	public void setDefaultGraph(String graph) {
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

}
