package org.bio_gene.wookie.connection;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface für die verschiedenen Connections
 * 
 * @author Felix Conrads
 *
 */
public interface Connection  {
	
	/**
	 * Typ für den Upload. 
	 * POST oder PUT
	 * 
	 * @author Felix Conrads
	 *
	 */
	public enum UploadType{
		POST, PUT
	}
	
	/**
	 * Ist autoCommit off (EXPERIMENTAL!!!) 
	 * kann mittels dem ModelUnionType entschieden werden 
	 * ob (noch nicht hochgeladene) Dateien (als Graphen)
	 * schlicht angefügt werden sollen oder vereinigt werden sollen
	 * 
	 * @author Felix Conrads
	 *
	 */
	public enum ModelUnionType{
		add, union
	}
	
	/**
	 * Läd ein File in den Triplestore
	 * Ist ein Default-Graph gesetzt wird in diesen hochgeladen
	 * 
	 * @param file File welches in den TS geladen werden soll
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long uploadFile(File file);
	/**
	 * Läd ein File in den Triplestore
	 * Ist ein Default-Graph gesetzt wird in diesen hochgeladen
	 * 
	 * @param fileName Name der Datei welche hochgeladen werden soll
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long uploadFile(String fileName);
	/**
	 * Läd ein File in den Triplestore
	 * Ist graphURI null und Default-Graph gesetzt wird in diesen hochgeladen
	 * 
	 * @param file File welches in den TS geladen werden soll
	 * @param graphURI Name des graphen in welches die Datei geladen werden soll
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long uploadFile(File file, String graphURI);
	/**
	 * Läd ein File in den Triplestore
	 * Ist graphURI null und Default-Graph gesetzt wird in diesen hochgeladen
	 * 
	 * @param fileName Name der Datei welche hochgeladen werden soll
	 * @param graphURI Name des graphen in welches die Datei geladen werden soll
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long uploadFile(String fileName, String graphURI);
	
	public long loadUpdate(String filename, String graphURI);
	
	public Long deleteFile(String file, String graphURI);
	
	public Long deleteFile(File file, String graphURI);
	/**
	 * EXPERIMENTAL!!!!
	 * Setzt ob autoCommit geschehen soll oder nicht
	 * (default=true)
	 * 
	 * @param autoCommit 
	 */
	public void autoCommit(Boolean autoCommit);
	/**
	 * EXPERIMENTAL!!!!
	 * Beginnt eine Transaction
	 */
	public void beginTransaction();
	/**
	 * EXPERIMENTAL!!!!
	 * Beendet eine Transaction und commitet die Änderungen
	 */
	public void endTransaction();
	
	/**
	 * Setzt ob POST oder PUT bei commit geschehen soll
	 * 
	 * @param type POST oder PUT
	 */
	public void setUploadType(UploadType type);
	/**
	 * Setzt den DefaultGraph
	 * 
	 * @param graph Name des Graphen
	 */
	public void setDefaultGraph(String graph);
	/**
	 * Setzt ob intern bei autoCommit (EXPERIMENTAL!!!)
	 * Hinzugefügt oder vereinigt werden soll
	 * 
	 * @param mut ADD oder UNION
	 */
	public void setModelUnionType(ModelUnionType mut);
	
	/**
	 * Schliesst die Connection
	 * @return true wenn erflogreich, andernfalss false
	 */
	public Boolean close();
	public Boolean isClosed() throws SQLException;
	
	public Long selectTime(String query, int queryTimeout) throws SQLException;
	
	public Long selectTime(String query) throws SQLException;
	
	public ResultSet select(String query, int queryTimeout) throws SQLException;
	
	/**
	 * Führt ein SELECT-query aus
	 * 
	 * @param query Query welche ausgeführt werden soll
	 * @return ResultSet zur query
	 * @throws SQLException
	 */
	public ResultSet select(String query) throws SQLException;
	/**
	 * Führt ein UPDATE-query aus
	 * 
	 * @param query Query welche ausgeführt werden soll
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long update(String query);
	
	public ResultSet execute(String query, int queryTimeout);
	
	/**
	 * Führt einen Query aus
	 * 
	 * @param query Query welche ausgeführt werden soll
	 * @return ResultSet falls Query eine SELECT Query, andernfalls null
	 */
	public ResultSet execute(String query);
	
	/**
	 * Dropped den angegeben Graph 
	 * 
	 * @param graphURI Name des Graphes welcher gedroppt werden soll
	 * @return true falls erfolgreich, andernfalls false
	 */
	public Long dropGraph(String graphURI);
	
	/**
	 * Setzt interne java.sql.Connection
	 * 
	 * @param con Neue java.sql.Connection
	 */
	public void setConnection(java.sql.Connection con);
	
	public String getEndpoint() ;

	public void setEndpoint(String endpoint);

	public String getUser() ;

	public void setUser(String user) ;

	public void setPwd(String pwd);
	public void setUpdateEndpoint(String updateEndpoint);
	
	public void setTriplesToUpload(long count);
	
	public long getTriplesToUpload();
}
