package org.bio_gene.wookie.connection;


public class ConnectionFactory {

	public static Connection createConnection(String xmlFile) {
		return null;
	}
	public static Connection createCurlConnection(String jdbcURL, String driver, String user, String password, String curlCommand, String curlDrop) {
		Connection con = new CurlConnection(jdbcURL, driver, user, password, curlCommand, curlDrop);
		connect(con);
		return con;
	}
	public static Connection createImplCurlConnection(String jdbcURL, String driver, String user, String password) {
		return null;
	}
	public static Connection createImplConnection(String jdbcURL, String driver, String user, String password) {
		return null;
	}
	public static Connection createCurlConnection(String jdbcURL, String driver, String curlCommand, String curlDrop) {
		Connection con = new CurlConnection(jdbcURL, driver, null, null, curlCommand, curlDrop);
		connect(con);
		return con;
	}
	public static Connection createImplCurlConnection(String jdbcURL, String driver) {
		return null;
	}
	public static Connection createImplConnection(String jdbcURL, String driver) {
		return null;
	}
	
	private static void connect(Connection con){
		
	}
	
}
