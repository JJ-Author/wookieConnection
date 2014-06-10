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

/**
 * Connection rein basierend auf dem SPARQL Endpoint des Triplestores
 * 
 * @author Felix Conrads
 *
 */
public class ImplConnection implements Connection {

	private java.sql.Connection con;
	private Logger log;
	private UploadType type = UploadType.POST;
	private Boolean autoCommit = true;
	private ModelUnionType mut = ModelUnionType.add;
	private Graph transactionInput;
	private String queries = "";
	private boolean transaction;
	
	/**
	 * ERSTELLT KEINE CONNECTION!
	 * ConnectionFactory benutzen!
	 */
	public ImplConnection(){
		Logger log = Logger.getLogger(this.getClass().getName());
		log.setLevel(Level.FINE);
		this.setLogger(log);
		LogHandler.initLogFileHandler(log, "Connection");
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
			Boolean ret =  this.update(update);
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

	
	private Boolean updateIntern(String query, Boolean drop){
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
			return false;
		}
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
	
	/**
	 * Basierend auf der internen java.sql.Connection (also über den Endpoint)
	 * wird ein SPARQL-UPDATE Befehl ausgeführt
	 * 
	 * @param query Auszuführende Query
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Boolean update(String query) {
		if(autoCommit){
			return updateIntern(query, false);
		}
		this.queries += query+="\n";
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
		if(commit()){
			this.transaction = false;
		}
	}

	private boolean commit() {
		//no need but i'm paranoid ;)
				if(this.transaction){
					return updateIntern(this.queries, false);
					
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

}
