package org.bio_gene.wookie.connection;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.bio_gene.wookie.utils.ConfigParser;
import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ConnectionFactory {
	
	private static String driver = "org.lexicon.jdbc4sparql.SPARQLDriver";
	private static String jdbcPrefix = "jdbc:sparql:http://";

	public enum ConnectionType{
		CURL, IMPLCURL, IMPL
	}
	
	@SuppressWarnings("unused")
	private static void setDriver(String driver){
		ConnectionFactory.driver = driver;
	}
	
	@SuppressWarnings("unused")
	private static void setJDBCPrefix(String jdbcPrefix){
		ConnectionFactory.jdbcPrefix = jdbcPrefix;
	}
	
	private static HashMap<String, String> getParams(Node databases, String dbId){
		HashMap<String, String> params = new HashMap<String, String>();
		try {
			ConfigParser cp = ConfigParser.getParser(databases);
			String id = "";
			if(!dbId.isEmpty() && dbId != null){
				id = dbId;
			}
			else if(((Element) databases).hasAttribute("main")){
				id =((Element) databases).getAttribute("main");
			}
			Element db = cp.getElementWithAttribute("id", id, "database");
			HashMap<String, String> ret= new HashMap<String, String>();
			ret.put("endpoint", cp.getElementAt("endpoint", 0).getAttribute("uri"));
			ret.put("user", cp.getElementAt("user", 0).getAttribute("value"));
			ret.put("pwd", cp.getElementAt("pwd", 0).getAttribute("value"));
			ret.put("connectionType", db.getAttribute("type").toUpperCase());
			switch(ConnectionType.valueOf(db.getAttribute("type").toUpperCase())){
			case CURL:
				ret.putAll(getCurlParams((Node)db));
				break;
			case IMPL:
				break;
			case IMPLCURL:
				ret.putAll(getImplCurlParams((Node)db));
				break;
			}
		} catch (SAXException | IOException | ParserConfigurationException e) {
			LogHandler.writeStackTrace(Logger.getGlobal(), e, Level.SEVERE);
			return null;
		}
		return params;
	}
	
	private static Map<String, String> getImplCurlParams(Node db) {
		try{	
			ConfigParser cp = ConfigParser.getParser(db);
			HashMap<String, String> ret= new HashMap<String, String>();
			ret.put("auth-type", cp.getElementAt("auth-type", 0).getAttribute("type"));
			NodeList nl = cp.getNodeList("properties");
			for(int i=0; i<nl.getLength();i++){
				Element e = (Element)nl.item(i);
				ret.put(e.getAttribute("name"), e.getAttribute("value"));
			}
			return ret;
		}
		catch(ParserConfigurationException | SAXException | IOException e){
			LogHandler.writeStackTrace(Logger.getGlobal(), e, Level.SEVERE);
			return null;
		}
	}

	private static Map<String, String> getCurlParams(Node db) {
		try{	
			ConfigParser cp = ConfigParser.getParser(db);
			HashMap<String, String> ret= new HashMap<String, String>();
			ret.put("curl-url", cp.getElementAt("curl-url", 0).getAttribute("url"));
			ret.put("curl-update", cp.getElementAt("curl-update", 0).getAttribute("command"));
			ret.put("curl-drop", cp.getElementAt("curl-drop", 0).getAttribute("command"));
			ret.put("curl-command", cp.getElementAt("curl-command", 0).getAttribute("command"));
			return ret;
		}
		catch(ParserConfigurationException | SAXException | IOException e){
			LogHandler.writeStackTrace(Logger.getGlobal(), e, Level.SEVERE);
			return null;
		}
	}
	
	public static Connection createConnection(String xmlFile) {
		return createConnection(xmlFile, null);
	}
	
	public static Connection createConnection(String xmlFile, String id) {
		try {
			ConfigParser cp = ConfigParser.getParser(xmlFile);
			cp.getElementAt("wookie", 0);
			Node databases = cp.getElementAt("databases", 0);
			return createConnection(databases, id);
		} catch (SAXException | IOException | ParserConfigurationException e) {
			LogHandler.writeStackTrace(Logger.getGlobal(), e, Level.SEVERE);
			return null;
		}
	}
	
	public static Connection createConnection(Node databases) {
		return createConnection(databases, null);
	}

	public static Connection createConnection(Node databases, String id) {
		HashMap<String, String> params = getParams(databases, id);
		switch(ConnectionType.valueOf(params.get("connectionType"))){
		case CURL:
			return createCurlConnection(params.get("endpoint"), params.get("user"),params.get("pwd"), 
					params.get("curl-command"), params.get("curl-drop"),params.get("curl--url"), params.get("curl-update"));
		case IMPL:
			return createImplConnection(params.get("endpoint"), params.get("user"),params.get("pwd"));
		case IMPLCURL:
			HashMap<String, String> props = new HashMap<String, String>();
			for(String key:params.keySet()){
				if(key.equals("endpoint")||key.equals("user")||key.equals("pwd") || key.equals("auth-type")){
					continue;
				}
				props.put(key, params.get(key));
			}
			return createImplCurlConnection(params.get("endpoint"), params.get("user"),params.get("pwd"), params.get("auth-type"), props);
		}
		return null;
	}
	
	public static Connection createCurlConnection(String endpoint,
			String user, String password, String curlCommand,
			String curlDrop, String curlURL, String curlUpdate){
		curlCommand = curlCommand.replace("%USER", user)
				.replace("%PWD", password)
				.replace("%ENDPOINT", endpoint)
				.replace("%CURL-URL", curlURL);
		curlDrop = curlDrop.replace("%USER", user)
				.replace("%PWD", password)
				.replace("%ENDPOINT", endpoint)
				.replace("%CURL-URL", curlURL);
		Connection con = new CurlConnection(endpoint,  user, password,
				curlCommand, curlDrop, curlURL, curlUpdate);
		con.setConnection(connect(endpoint, ConnectionFactory.driver, user, password));
		return con;
	}
	
	public static Connection createCurlConnection(String endpoint,
			String user, String password, String curlCommand,
			String curlDrop, String curlUpdate) {
		return ConnectionFactory.createCurlConnection(endpoint,
				user, password,  curlCommand,
				curlDrop, null, curlUpdate);
	}

	public static Connection createCurlConnection(String endpoint,
			String curlCommand, String curlDrop, String curlURL, String curlUpdate) {

		return ConnectionFactory.createCurlConnection(endpoint,
				null, null,  curlCommand,
				curlDrop, curlURL, curlUpdate);
	}
	
	public static Connection createCurlConnection(String endpoint,
			String curlCommand, String curlDrop, String curlUpdate) {

		return ConnectionFactory.createCurlConnection(endpoint,
				null, null,  curlCommand,
				curlDrop, null, curlUpdate);
	}
	
	
	
	public static Connection createImplCurlConnection(String endpoint, String authType, 
			String user, String password, HashMap<String, String> props) {
		Connection con = new ImplCurlConnection(authType, props);
		con.setConnection(connect(endpoint, ConnectionFactory.driver, user, password));
		return con;
	}
	
	public static Connection createImplCurlConnection(String endpoint, 
			String user, String password, HashMap<String, String> props) {
		return createImplCurlConnection(endpoint, null, user, password, props);
	}
	
	public static Connection createImplCurlConnection(String endpoint, String authType,
			String user, String password) {
		return createImplCurlConnection(endpoint, authType, user, password, null);
	}
	
	public static Connection createImplCurlConnection(String endpoint, String authType) {
		return createImplCurlConnection(endpoint, authType, null, null, null);
	}
	
	public static Connection createImplCurlConnection(String endpoint,
			String user, String password) {
		return createImplCurlConnection(endpoint, null, user, password, null);
	}
	
	public static Connection createImplCurlConnection(String endpoint) {
		return createImplCurlConnection(endpoint, null, null, null, null);
	}
	
	
	
	public static Connection createImplConnection(String endpoint,
			String user, String password) {
		Connection con = new ImplConnection();
		con.setConnection(connect(endpoint, ConnectionFactory.driver, user, password));
		return con;
	}

	public static Connection createImplConnection(String endpoint) {
		return createImplConnection(endpoint, null, null);
	}

	private static java.sql.Connection connect(String endpoint, String driver,
			String user, String pwd) {

		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			LogHandler.writeStackTrace(Logger.getGlobal(), e, Level.SEVERE);
			return null;
		}

		java.sql.Connection con = null;
		try {
			if (user != null && pwd != null) {
				// Connects with the given jdbcURL as user with pwd

				con = DriverManager.getConnection(jdbcPrefix+endpoint, user, pwd);

				// AS some Triplestores can't connect as a user without setting
				// the user explicit we need this
				((SPARQLConnection) con).setUsername(user);
			} else {
				// Connects with the given jdbcURL
				con = DriverManager.getConnection(jdbcPrefix+endpoint);

			}
		} catch (SQLException e) {
			LogHandler.writeStackTrace(Logger.getGlobal(), e, Level.SEVERE);
			return null;
		}
		return con;
	}

}
