package org.bio_gene.wookie.connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.jena.jdbc.remote.RemoteEndpointDriver;
import org.bio_gene.wookie.utils.ConfigParser;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

/**
 * Erstellt gewünschte Connection
 * 
 * für XMLFile Description: \<wookie\> \<databases main="$ID"\> CurlConnection:
 * \<database id="curl" type="CURL"\> \<endpoint
 * uri="sub1.example.com/sparql"/\> \<user value="dba" /\> //optional \<pwd
 * value="secret" /\> //optional \<curl-url
 * url="http://sub2.example.com/data/update"/\> //optional \<curl-update
 * command=
 * "curl --date-urlencode --digest $USER:$PWD '$UPDATE' --url '$CURL-URL'"/\>
 * \<curl-command command=
 * "curl --digset $USER:$PWD --url '$CURL-URL' -X $UPLOAD-TYPE -T $FILE"/\>
 * \<curl-drop command="curl -X DELETE $GRAPH-URI --url '$CURL-URL'"/\>
 * \</database\>
 * 
 * ImplConnection: \<database id="curl" type="IMPL"\> \<endpoint
 * uri="sub1.example.com/sparql"/\> \<user value="dba" /\> //optional \<pwd
 * value="secret" /\> //optional \</database\>
 * 
 * ImplCurlConnection: \<database id="curl" type="IMPLCURL"\> \<endpoint
 * uri="sub1.example.com/sparql"/\> \<user value="dba" /\> //optional \<pwd
 * value="secret" /\> //optional \<auth-type type="{basic|digest}" /\>
 * //optional //any property which is a curl parameter can set here with its
 * value \<property name="curl-property_1" value="example1"/\> //optional
 * \<property name="curl-property_2" value="example2"/\> //optional ....
 * \<property name="curl-property_n" value="example3"/\> //optional
 * \</database\>
 * 
 * \</databases\> \</wookie\>
 * 
 * @author Felix Conrads
 * 
 */
public class ConnectionFactory {


	private static String driver = "org.apache.jena.jdbc.remote.RemoteEndpointDriver";
	private static String jdbcPrefix = "jdbc:jena:remote:query=http://";

	private static Logger log = Logger.getLogger(ConnectionFactory.class.getSimpleName());
	
	static {
		LogHandler.initLogFileHandler(log, ConnectionFactory.class.getSimpleName());
	}
	
	// private static String driver = "org.xenei.jdbc4sparql.J4SDriver";
	// private static String jdbcPrefix = "jdbc:j4s:http://";
	/**
	 * Implementierte Connection Typen
	 * 
	 * @author Felix Conrads
	 * 
	 */
	public enum ConnectionType {
		CURL, IMPLCURL, IMPL, FEDERATED
	}

	/**
	 * Soll ein anderer JDBCDriver benutzt werden muss dieser hier vor
	 * Initialisierung der Connection geändert werden.
	 * 
	 * Momentan funktioniert es noch nicht einen anderen JDBC Treiber zu
	 * benutzen
	 * 
	 * @param driver
	 */
	public static void setDriver(String driver) {
		ConnectionFactory.driver = driver;
	}

	/**
	 * Soll ein anderer JDBCDriver benutzt werden muss der jdbcPrefix richtig
	 * gesetzt werden
	 * 
	 * Momentan funktioniert es noch nicht einen anderen JDBC Treiber zu
	 * benutzen
	 * 
	 * @param jdbcPrefix
	 */
	public static void setJDBCPrefix(String jdbcPrefix) {
		ConnectionFactory.jdbcPrefix = jdbcPrefix;
	}

	private static HashMap<String, String> getParams(Node databases, String dbId) {
		HashMap<String, String> params = new HashMap<String, String>();
		try {
			ConfigParser cp = ConfigParser.getParser(databases);
			String id = "";
			Element db = null;
			if (dbId != null && !dbId.isEmpty()) {
				id = dbId;
				db = cp.getElementWithAttribute("id", id, "database");
			} else if (((Element) databases).hasAttribute("main")) {
				id = ((Element) databases).getAttribute("main");
				db = cp.getElementWithAttribute("id", id, "database");
			} else {
				db = cp.getElementAt("database", 0);
			}

			HashMap<String, String> ret = new HashMap<String, String>();
			cp.setNode((Element) db);
			ret.put("endpoint",
					cp.getElementAt("endpoint", 0).getAttribute("uri"));
			
			cp.setNode((Element) db);
			try{
				ret.put("queryTimeout",
						cp.getElementAt("queryTimeout", 0).getAttribute("value"));
			}catch(Exception e){
				ret.put("queryTimeout", "180");
			}
			cp.setNode((Element) db);
			try{
				ret.put("update-endpoint", cp.getElementAt("update-endpoint", 0)
						.getAttribute("uri"));
			}
				catch(Exception e){
					ret.put("update-endpoint", ret.get("endpoint"));
				}
			cp.setNode((Element) db);
			try {
				ret.put("user", cp.getElementAt("user", 0)
						.getAttribute("value"));
				cp.setNode((Element) db);
				ret.put("pwd", cp.getElementAt("pwd", 0).getAttribute("value"));
				cp.setNode((Element) db);
			} catch (Exception e) {
				log.info("No User and PWD is set - correct?");
			}
			ret.put("connectionType", db.getAttribute("type").toUpperCase());
			switch (ConnectionType.valueOf(db.getAttribute("type")
					.toUpperCase())) {
			case CURL:
				ret.putAll(getCurlParams((Node) db));
				break;
			case IMPL:
				break;
			}
			params.putAll(ret);
		} catch (SAXException | IOException | ParserConfigurationException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
		return params;
	}



	private static Map<String, String> getCurlParams(Node db) {
		try {
			ConfigParser cp = ConfigParser.getParser(db);
			HashMap<String, String> ret = new HashMap<String, String>();
			try {
				ret.put("curl-url", cp.getElementAt("curl-url", 0)
						.getAttribute("url"));
			} catch (Exception e) {
				cp.setNode((Element) db);
				ret.put("curl-url", cp.getElementAt("endpoint", 0)
						.getAttribute("uri"));
			}
			cp.setNode((Element) db);
			try{
				ret.put("curl-update", cp.getElementAt("curl-update", 0)
						.getAttribute("command"));
			}
			catch(Exception e){}
			cp.setNode((Element) db);
			try{
			ret.put("curl-drop",
					cp.getElementAt("curl-drop", 0).getAttribute("command"));
			}
			catch(Exception e){}
			cp.setNode((Element) db);
			ret.put("curl-command", cp.getElementAt("curl-command", 0)
					.getAttribute("command"));
			cp.setNode((Element) db);
			
			return ret;
		} catch (ParserConfigurationException | SAXException | IOException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	}

	/**
	 * Erstellt eine Connection aus einem xmlFile aufgebaut wie folgt: Hierbei
	 * muss im tag databases main mit der gewünschten Id der Connection gesetzt
	 * werden
	 * 
	 * \<wookie\> \<databases main="abc"\> \<database id="a" type="CURL"\> ....
	 * \</database\> \<database id="b" type="IMPL"\> .... \</database\>
	 * \<database id="abc" type="IMPLCURL"\> //Diese DB wird genommen
	 * \</database\> \</databases\> \</wookie\>
	 * 
	 * @param xmlFile
	 *            Name der XMLDatei
	 * @return Connection zum ausgewähltem TripleStore
	 */
	public static Connection createConnection(String xmlFile) {
		return createConnection(xmlFile, null);
	}

	/**
	 * Erstellt eine Connection aus einem xmlFile aufgebaut wie folgt: Hierbei
	 * wird die Connection mit der angegeben ID genommen
	 * 
	 * Bsp: id=b
	 * 
	 * \<wookie\> \<databases main="abc"\> \<database id="a" type="CURL"\> ....
	 * \</database\> \<database id="b" type="IMPL"\> //Diese DB wird genommen
	 * \</database\> \<database id="abc" type="IMPLCURL"\> .... \</database\>
	 * \</databases\> \</wookie\>
	 * 
	 * @param xmlFile
	 *            Name der XMLDatei
	 * @param id
	 *            id der Database im tag database
	 * @return Connection zum ausgewähltem TripleStore
	 */
	public static Connection createConnection(String xmlFile, String id) {
		try {
			ConfigParser cp = ConfigParser.getParser(xmlFile);
			cp.getElementAt("wookie", 0);
			Node databases = cp.getElementAt("databases", 0);
			return createConnection(databases, id);
		} catch (SAXException | IOException | ParserConfigurationException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	}

	/**
	 * Erstellt eine Connection zugehörig zu einem Knoten databases: Hierbei
	 * muss im tag databases main mit der gewünschten Id der Connection gesetzt
	 * werden
	 * 
	 * \<databases main="abc"\> \<database id="a" type="CURL"\> ....
	 * \</database\> \<database id="b" type="IMPL"\> .... \</database\>
	 * \<database id="abc" type="IMPLCURL"\> //Diese DB wird genommen
	 * \</database\> \</databases\>
	 * 
	 * @param databases
	 *            Node mit den beinhalteten Triplestores
	 * @return Connection zum ausgewählten Triplestore
	 */
	public static Connection createConnection(Node databases) {
		return createConnection(databases, null);
	}

	/**
	 * Erstellt eine Connection aus einem xmlFile aufgebaut wie folgt: Hierbei
	 * wird die Connection mit der angegeben ID genommen
	 * 
	 * Bsp: id=b
	 * 
	 * \<wookie\> \<databases main="abc"\> \<database id="a" type="CURL"\> ....
	 * \</database\> \<database id="b" type="IMPL"\> //Diese DB wird genommen
	 * \</database\> \<database id="abc" type="IMPLCURL"\> .... \</database\>
	 * \</databases\> \</wookie\>
	 * 
	 * database kann com Typ IMPL, IMPLCURL oder CURL sein.
	 * 
	 * @param databases
	 *            Node mit den beinhalteten Triplestores
	 * @param id
	 *            id der Database im tag database
	 * @return Connection zum ausgewähltem TripleStore
	 */
	public static Connection createConnection(Node databases, String id) {
		HashMap<String, String> params = getParams(databases, id);
		switch (ConnectionType.valueOf(params.get("connectionType"))) {
		case CURL:
			return createCurlConnection(params.get("endpoint"),
					params.get("user"), params.get("pwd"),
					params.get("curl-command"), params.get("curl-drop"),
					params.get("curl-url"), params.get("curl-update"),
					params.get("update-endpoint"), 
					Integer.valueOf(params.get("queryTimeout")));
		case IMPL:
			return createImplConnection(params.get("endpoint"),
					params.get("user"), params.get("pwd"), params.get("update-endpoint"), 
					Integer.valueOf(params.get("queryTimeout")));
		case FEDERATED:
			
			return createFederatedConnection(params.get("endpoint"),
					params.get("user"), params.get("pwd"), params.get("update-endpoint"), 
					Integer.valueOf(params.get("queryTimeout")));
		}
		return null;
	}

	public static Connection createFederatedConnection(String endpoint,
			String user, String pwd, String updateEndpoint, Integer queryTimeOut) {
		Connection con = new FederatedConnection(queryTimeOut);
		String[] users = user.split(";");
		String[] pwds = pwd.split(";");
		String[] updates = updateEndpoint.split(";");
		con.setEndpoint("http://"+endpoint);
		con.setConnection(connect(endpoint, null, ConnectionFactory.driver, null, null));
		for(int i=0;i<updates.length;i++){
			con.setPwd(pwds[i]);
			con.setUser(users[i]);
			con.setUpdateEndpoint("http://"+updates[i]);
			con.setConnection(connect(endpoint, updates[i], ConnectionFactory.driver, users[i],
					pwds[i]));
		}
		return con;
	}

	/**
	 * Erstellt eine CurlConnection mit den angegeben Parametern Es werden die
	 * angebenen Befehle als Script ausgeführt s. CurlConnection
	 * 
	 * Kommandos müssen folgende Variablen besitzen damit diese ersetzt werden
	 * können: $USER $PWD $ENDPOINT $CURL-URL $GRAPH-URI $CONTENT-TYPE
	 * $MIME-TYPE $UPLOAD-TYPE $FILE $UPDATE Diese werden dann in den Befehlen
	 * ersetzt um größtmögliche Dynamik und Redundanz vermeidung zu ermöglichen
	 * (Alle Variablen sind optional)
	 * 
	 * @param endpoint
	 *            SPARQL-endpoint vom Triplestore ohne "http://"
	 * @param user
	 *            user des Triplestores
	 * @param password
	 *            pwd des Triplestores
	 * @param curlCommand
	 *            curl (oder script) Kommando um Datei in TS zu laden
	 * @param curlDrop
	 *            curl (oder script) Kommando um Graph zu löschen
	 * @param curlURL
	 *            URL für die curl Befehle (da dieser unterschiedlich zu
	 *            endpoint sein kann)
	 * @param curlUpdate
	 *            curl (oder script) Kommando um einen SPARQL Update befehl
	 *            auszuführen
	 * @return Connection zum angebene Triplestore
	 */
	public static Connection createCurlConnection(String endpoint, String user,
			String password, String curlCommand, String curlDrop,
			String curlURL, String curlUpdate, String updateEndpoint, int queryTimeout) {
		if (curlURL == null) {
			curlURL = endpoint;
		}
		if(user==null)
			user="";
		if(endpoint==null)
			endpoint="";
		if(curlURL==null)
			curlURL="";
		if(password==null)
			password="";
		if(updateEndpoint==null)
			updateEndpoint = endpoint;
		
		curlCommand = curlCommand.replace("$USER", user)
				.replace("$PWD", password).replace("$ENDPOINT", endpoint)
				.replace("$CURL-URL", curlURL);
		curlDrop = curlDrop.replace("$USER", user).replace("$PWD", password)
				.replace("$ENDPOINT", endpoint).replace("$CURL-URL", curlURL);
		Connection con = new CurlConnection(endpoint, user, password,
				curlCommand, curlDrop, curlURL, curlUpdate, updateEndpoint, queryTimeout);
		con.setConnection(connect(endpoint, updateEndpoint, ConnectionFactory.driver, user,
				password));
		return con;
	}

	/**
	 * Erstellt eine CurlConnection mit den angegeben Parametern Es werden die
	 * angebenen Befehle als Script ausgeführt s. CurlConnection
	 * 
	 * Kommandos müssen folgende Variablen besitzen damit diese ersetzt werden
	 * können: $USER $PWD $ENDPOINT $CURL-URL wird hier mittels endpoint ersetzt
	 * $GRAPH-URI $CONTENT-TYPE $MIME-TYPE $UPLOAD-TYPE $FILE $UPDATE Diese
	 * werden dann in den Befehlen ersetzt um größtmögliche Dynamik und
	 * Redundanz vermeidung zu ermöglichen (Alle Variablen sind optional)
	 * 
	 * @param endpoint
	 *            SPARQL-endpoint vom Triplestore ohne "http://"
	 * @param user
	 *            user des Triplestores
	 * @param password
	 *            pwd des Triplestores
	 * @param curlCommand
	 *            curl (oder script) Kommando um Datei in TS zu laden
	 * @param curlDrop
	 *            curl (oder script) Kommando um Graph zu löschen
	 * @param curlUpdate
	 *            curl (oder script) Kommando um einen SPARQL Update befehl
	 *            auszuführen
	 * @return Connection zum angebene Triplestore
	 */
	public static Connection createCurlConnection(String endpoint, String user,
			String password, String curlCommand, String curlDrop,
			String curlUpdate, String updateEndpoint, int queryTimeout) {
		return ConnectionFactory.createCurlConnection(endpoint, user, password,
				curlCommand, curlDrop, endpoint, curlUpdate, updateEndpoint, queryTimeout);
	}

	/**
	 * Erstellt eine CurlConnection mit den angegeben Parametern Es werden die
	 * angebenen Befehle als Script ausgeführt s. CurlConnection
	 * 
	 * Kommandos müssen folgende Variablen besitzen damit diese ersetzt werden
	 * können: $ENDPOINT $CURL-URL $GRAPH-URI $CONTENT-TYPE $MIME-TYPE
	 * $UPLOAD-TYPE $FILE $UPDATE Diese werden dann in den Befehlen ersetzt um
	 * größtmögliche Dynamik und Redundanz vermeidung zu ermöglichen (Alle
	 * Variablen sind optional)
	 * 
	 * @param endpoint
	 *            SPARQL-endpoint vom Triplestore ohne "http://"
	 * @param curlCommand
	 *            curl (oder script) Kommando um Datei in TS zu laden
	 * @param curlDrop
	 *            curl (oder script) Kommando um Graph zu löschen
	 * @param curlURL
	 *            URL für die curl Befehle (da dieser unterschiedlich zu
	 *            endpoint sein kann)
	 * @param curlUpdate
	 *            curl (oder script) Kommando um einen SPARQL Update befehl
	 *            auszuführen
	 * @return Connection zum angebene Triplestore
	 */
	public static Connection createCurlConnection(String endpoint,
			String curlCommand, String curlDrop, String curlURL,
			String curlUpdate, String updateEndpoint, int queryTimeout) {

		return ConnectionFactory.createCurlConnection(endpoint, null, null,
				curlCommand, curlDrop, curlURL, curlUpdate, updateEndpoint, queryTimeout);
	}

	/**
	 * Erstellt eine CurlConnection mit den angegeben Parametern Es werden die
	 * angebenen Befehle als Script ausgeführt s. CurlConnection
	 * 
	 * Kommandos müssen folgende Variablen besitzen damit diese ersetzt werden
	 * können: $ENDPOINT $CURL-URL - wird hier mittels endpoint ersetzt
	 * $GRAPH-URI $CONTENT-TYPE $MIME-TYPE $UPLOAD-TYPE $FILE $UPDATE Diese
	 * werden dann in den Befehlen ersetzt um größtmögliche Dynamik und
	 * Redundanz vermeidung zu ermöglichen (Alle Variablen sind optional)
	 * 
	 * @param endpoint
	 *            SPARQL-endpoint vom Triplestore ohne "http://"
	 * @param curlCommand
	 *            curl (oder script) Kommando um Datei in TS zu laden
	 * @param curlDrop
	 *            curl (oder script) Kommando um Graph zu löschen
	 * @param curlUpdate
	 *            curl (oder script) Kommando um einen SPARQL Update befehl
	 *            auszuführen
	 * @return Connection zum angebene Triplestore
	 */
	public static Connection createCurlConnection(String endpoint,
			String curlCommand, String curlDrop, String curlUpdate, String updateEndpoint, int queryTimeout) {

		return ConnectionFactory.createCurlConnection(endpoint, null, null,
				curlCommand, curlDrop, endpoint, curlUpdate, updateEndpoint, queryTimeout);
	}




	/**
	 * Erstellt eine auf SPARQL Update basierende Connection
	 * 
	 * @param endpoint
	 *            SPARQL Endpoint des Triplestores
	 * @param user
	 *            User name
	 * @param password
	 *            password des Users
	 * @return Connection zum gewünschten Triplestore
	 */
	public static Connection createImplConnection(String endpoint, String user,
			String password, String updateEndpoint, int queryTimeout) {
		Connection con = new ImplConnection(queryTimeout);
		con.setPwd(password);
		con.setUser(user);
		con.setEndpoint("http://"+endpoint);
		if(updateEndpoint==null)
			updateEndpoint = endpoint;
		con.setUpdateEndpoint("http://"+updateEndpoint);
		con.setConnection(connect(endpoint, updateEndpoint, ConnectionFactory.driver, user,
				password));
		return con;
	}

	/**
	 * Erstellt eine auf SPARQL Update basierende Connection
	 * 
	 * @param endpoint
	 *            SPARQL Endpoint des Triplestores
	 * @return Connection zum gewünschten Triplestore
	 */
	public static Connection createImplConnection(String endpoint, String updateEndpoint, int queryTimeout) {
		return createImplConnection(endpoint, null, null, updateEndpoint, queryTimeout);
	}

	@SuppressWarnings("unused")
	private static java.sql.Connection connect2(String endpoint, String driver,
			String user, String pwd) {

		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}

		java.sql.Connection con = null;
		try {
			if (user != null && pwd != null) {
				// Connects with the given jdbcURL as user with pwd

				con = DriverManager.getConnection(jdbcPrefix + endpoint, user,
						pwd);

				// AS some Triplestores can't connect as a user without setting
				// the user explicit we need this
				if (con instanceof SPARQLConnection) {
					((SPARQLConnection) con).setUsername(user);
				}
			} else {
				// Connects with the given jdbcURL
				con = DriverManager.getConnection(jdbcPrefix + endpoint);

			}
		} catch (SQLException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
		return con;
	}

	private static java.sql.Connection connect(String endpoint, String updateEndpoint, String driver,
			String user, String pwd) {

		// Dem DriverManager den Driver der DBMS geben.
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
		
		SPARQLConnection con = null;
		try {
//			java.sql.DriverManager.registerDriver(new J4SDriver());
			RemoteEndpointDriver.register();
		} catch (SQLException e1) {
			LogHandler.writeStackTrace(log, e1, Level.SEVERE);
			return null;
		}
		java.sql.Connection internCon = null;
		// Ist user und pwd nicht null, soll die Verbindung mit user und pwd
		// geschehen
		String url = jdbcPrefix+endpoint;
		if(driver.equals("org.apache.jena.jdbc.remote.RemoteEndpointDriver")){
			jdbcPrefix="jdbc:jena:remote:query=http://"+endpoint
					+"&update=http://"+updateEndpoint;
			url = jdbcPrefix;
		}
		
		
		
		try {
		if (user != null && pwd != null) {
			// Verbindet mit der DB
			Properties info = new Properties();
			info.put("user", user);
			info.put("password", pwd);
			if(driver.equals("org.apache.jena.jdbc.remote.RemoteEndpointDriver")){
				url+="&user="+user+"&password="+pwd;
			}
			internCon = DriverManager.getConnection(url, info);
			// Virtuoso braucht den Usernamen explizit und akzeptiert den nicht
			// in der URI
			if(internCon instanceof SPARQLConnection){
				con = (SPARQLConnection) internCon;
				con.setUsername(user);
			}
		} else {
			// Verbindet mit der DB
			internCon = DriverManager.getConnection(url);
			if(internCon instanceof SPARQLConnection){
				con = (SPARQLConnection) internCon;
			}
		}
		} catch (SQLException e) {
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			return null;
		}
	
		
		if(con!=null){
			
			return con;
		}
		return internCon;
	}

}
