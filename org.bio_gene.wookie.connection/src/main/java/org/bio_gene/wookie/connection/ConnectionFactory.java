package org.bio_gene.wookie.connection;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;
import org.w3c.dom.Node;

public class ConnectionFactory {
	
	private static String driver = "org.lexicon.jdbc4sparql.SPARQLDriver";
	private static String jdbcPrefix = "jdbc:sparql:http://";

	@SuppressWarnings("unused")
	private static void setDriver(String driver){
		ConnectionFactory.driver = driver;
	}
	
	@SuppressWarnings("unused")
	private static void setJDBCPrefix(String jdbcPrefix){
		ConnectionFactory.jdbcPrefix = jdbcPrefix;
	}
	
	private static HashMap<String, String> getParams(Node databases){
		HashMap<String, String> params = new HashMap<String, String>();
		
		return params;
	}
	
	public static Connection createConnection(String xmlFile) {
		return null;
	}
	
	public static Connection createConnection(Node databases) {
		HashMap<String, String> params = getParams(databases);
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
	
	
	
	public static Connection createImplCurlConnection(String jdbcURL,
			String user, String password) {
		return null;
	}

	public static Connection createImplConnection(String jdbcURL,
			String user, String password) {
		return null;
	}



	public static Connection createImplCurlConnection(String jdbcURL,
			String driver) {
		return null;
	}

	public static Connection createImplConnection(String jdbcURL, String driver) {
		return null;
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
