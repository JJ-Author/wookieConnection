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
import java.util.LinkedList;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.jena.riot.RDFLanguages;
import org.bio_gene.wookie.utils.CurlProcess;
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.GraphHandler;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * Connection basierend auf Curl Prozessen (auch anderen Scripten).
 * 
 * @author Felix Conrads
 *
 */
public class CurlConnection extends CurlProcess implements Connection {

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
	private Collection<String> dropGraphs;
	private UploadType type =  UploadType.POST;
	private ModelUnionType mut = ModelUnionType.add;
	
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
			String curlURL, String curlUpdate){
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
		this.curlCommand = curlCommand;
		this.curlDrop = curlDrop;
		this.curlURL = curlURL!=null? curlURL : endpoint;
		this.curlUpdate = curlUpdate;
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
				return false;
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
		return this.process(command);
	}
	
	
	/**
	 * Läd die angegebene Datei in den Default Graph sofern in curlCommand $GRAPH-URI gesetzt ist
	 * 
	 * @param file Das hochzuladene File
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Boolean uploadFile(File file) {
		return uploadFileIntern(file, ((SPARQLConnection)this.con).getDefaultGraphs().get(0));
	}

	/**
	 * Läd die angegebene Datei in den Default Graph sofern in curlCommand $GRAPH-URI gesetzt ist
	 * 
	 * @param fileName Name der Hochzuladenen Datei
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Boolean uploadFile(String fileName) {
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
	public Boolean uploadFile(File file, String graphURI) {
		return uploadFileIntern(file, graphURI);
	}

	/**
	 * Läd die angegebene Datei in den angegebenen Graph sofern in curlCommand $GRAPH-URI gesetzt ist
	 * 
	 * @param fileName Name der hochzuladenen Datei
	 * @param graphURI Graph in den geladen werden soll
	 * @return true wenn erfolgreich, false falls misserfolg, null falls autoCommit gleich false 
	 */
	public Boolean uploadFile(String fileName, String graphURI) {
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
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird ein SPARQL-UPDATE Befehl ausgeführt
	 * (unabhängig von autoCommit und uploadType )
	 * 
	 * @param query Auszuführende Query
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Boolean update(String query) {
		/*
		 *TODO  Update and execute must be written in Curl! Update must ask if autoCommit
		 */
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
		this.dropGraphs.add(graphURI);
		if(this.autoCommit){
			//TODO ändern!!!
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
	

}
