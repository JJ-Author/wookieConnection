package org.bio_gene.wookie.connection;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bio_gene.wookie.utils.LogHandler;
import org.lexicon.jdbc4sparql.SPARQLConnection;

public class ConnectionFactory {
	
	private static String driver = "org.lexicon.jdbc4sparql.SPARQLDriver";

	public static Connection createConnection(String xmlFile) {
		return null;
	}

	public static Connection createCurlConnection(String jdbcURL,
			String driver, String user, String password, String curlCommand,
			String curlDrop) {
		Connection con = new CurlConnection(jdbcURL, driver, user, password,
				curlCommand, curlDrop);
		con.setConnection(connect(jdbcURL, driver, user, password));
		return con;
	}

	public static Connection createImplCurlConnection(String jdbcURL,
			String driver, String user, String password) {
		return null;
	}

	public static Connection createImplConnection(String jdbcURL,
			String driver, String user, String password) {
		return null;
	}

	public static Connection createCurlConnection(String jdbcURL,
			String driver, String curlCommand, String curlDrop) {

		Connection con = new CurlConnection(jdbcURL, driver, null, null,
				curlCommand, curlDrop);
		con.setConnection(connect(jdbcURL, driver, null, null));
		return con;
	}

	public static Connection createImplCurlConnection(String jdbcURL,
			String driver) {
		return null;
	}

	public static Connection createImplConnection(String jdbcURL, String driver) {
		return null;
	}

	private static java.sql.Connection connect(String jdbcURL, String driver,
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

				con = DriverManager.getConnection(jdbcURL, user, pwd);

				// AS some Triplestores can't connect as a user without setting
				// the user explicit we need this
				((SPARQLConnection) con).setUsername(user);
			} else {
				// Connects with the given jdbcURL
				con = DriverManager.getConnection(jdbcURL);

			}
		} catch (SQLException e) {
			LogHandler.writeStackTrace(Logger.getGlobal(), e, Level.SEVERE);
			return null;
		}
		return con;
	}

}
