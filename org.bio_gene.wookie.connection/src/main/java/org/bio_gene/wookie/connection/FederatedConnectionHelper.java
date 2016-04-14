package org.bio_gene.wookie.connection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
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
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.FileHandler;
import org.bio_gene.wookie.utils.LogHandler;

import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.modify.UpdateProcessRemoteForm;
import org.apache.jena.sparql.modify.request.UpdateLoad;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import com.ibm.icu.util.Calendar;

public class FederatedConnectionHelper implements Runnable {

	private ResultSet rsSelect;
	private Methods method;
	private Logger log;
	private long numberOfTriples;
	private long updateTime;
	private String filename;
	private String graphURI;
	private String user;
	private String pwd;
	private String updateEndpoint;
	private File file;
	private String query;
	private int queryTimeout;
	private Connection con;
	private Long resTime=-1L;
	
	
	
	public enum Methods{
		UPLOAD, LOAD, SELECT, UPDATE, DELETE
	};
	
	private long loadUpdateIntern(String filename, String graphURI, String user, String pwd, String updateEndpoint){
		HttpContext httpContext = new BasicHttpContext();
		if(user!=null && pwd!=null){
			CredentialsProvider provider = new BasicCredentialsProvider();
			
			provider.setCredentials(new AuthScope(AuthScope.ANY_HOST,
					AuthScope.ANY_PORT), new UsernamePasswordCredentials(user, pwd));
			httpContext.setAttribute(ClientContext.CREDS_PROVIDER, provider);
		}

//		GraphStore graphStore = GraphStoreFactory.create() ;
		UpdateRequest request = UpdateFactory.create();
		request.add(new UpdateLoad(filename, graphURI));
		UpdateProcessor processor = UpdateExecutionFactory
			    .createRemoteForm(request, updateEndpoint);
			((UpdateProcessRemoteForm)processor).setHttpContext(httpContext);
		Long a = new Date().getTime();
			processor.execute();
		Long b = new Date().getTime();	
		return b-a;
	}
	
	private Long deleteFileIntern(File f, String graphURI, String user, String pwd, String updateEndpoint) throws IOException {
		String query = "";
		query="DELETE WHERE {";
		if(graphURI!=null){
			query+=" GRAPH <"+graphURI+"> { ";
		}
		FileReader fis = new FileReader(f);
		BufferedReader bis = new BufferedReader(fis);
		String triple="";
		while((triple=bis.readLine())!=null){
			query+=triple;
		}
		bis.close();
		if(graphURI!=null){
			query+=" }";
		}
		query+=" }";
		return ownUpdate(query, user, pwd, updateEndpoint);
		
	}
	
	private ResultSet selectIntern(String query, int queryTimeout, java.sql.Connection con){
		try{
			Statement stm = con.createStatement();
			stm.setQueryTimeout(queryTimeout);
			ResultSet rs=null;
			
			Calendar start = Calendar.getInstance();
			rs = stm.executeQuery(query);
			Calendar end = Calendar.getInstance();
			if(rs==null){
				setResTime(-1L);
			}
			else{
				setResTime(end.getTimeInMillis()-start.getTimeInMillis());
			}
//			stm.close();
			return rs;
		}
		catch(SQLException e){
			log.warning("Query doesn't work: "+query);
//			log.warning("For Connection: "+endpoint);
			LogHandler.writeStackTrace(log, e, Level.SEVERE);
			setResTime(-1L);
			return null;
		}
	}

	
	private Long uploadFileIntern(File file, String graphURI, String user, String pwd, String updateEndpoint){
		if(!file.exists()){
				LogHandler.writeStackTrace(log, new FileNotFoundException(), Level.SEVERE);
				return -1L;
		}
		Long ret = 0L;
		Boolean isFile=false;
		if(numberOfTriples<1){
			numberOfTriples =FileHandler.getLineCount(file);
			isFile=true;
		}
		try {
			for(File f : FileHandler.splitTempFile(file, numberOfTriples)){
				Long retTmp = uploadFileIntern2(f, graphURI, user, pwd, updateEndpoint);
				if(retTmp==-1){
					log.severe("Couldn't upload part: "+f.getName());
				}else{
					ret+=retTmp;
				}
				if(!isFile)
					f.delete();
			}
		} catch (IOException e) {
			log.severe("Couldn't upload file(s) due to: ");
			ret =-1L;
		}
		return ret;
	}
	
	
	private Long uploadFileIntern2(File file, String graphURI, String user, String pwd, String updateEndpoint) {
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
		if(FileExtensionToRDFContentTypeMapper.guessFileFormat(contentType)=="N-TRIPLE"){
			String update = "INSERT DATA ";
			if(graphURI !=null){
				update+=" { GRAPH <"+graphURI+"> ";
			}
			try {
				update+="{";
				FileReader fis = new FileReader(file);
				BufferedReader bis = new BufferedReader(fis);
				String triple="";
				while((triple=bis.readLine())!=null){
					update+=triple;
				}
				if(graphURI!=null){
					update+="}";
				}
				update+="}";
				Long ret =  ownUpdate(update, user, pwd, updateEndpoint);
				bis.close();
				return ret;
			} catch (IOException e) {
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
				return null;
			}	
		}
		return null;
	}
	
	private long ownUpdate(String query, String user, String pwd, String updateEndpoint){
		HttpContext httpContext = new BasicHttpContext();
		if(user!=null && pwd!=null){
			CredentialsProvider provider = new BasicCredentialsProvider();
			
			provider.setCredentials(new AuthScope(AuthScope.ANY_HOST,
					AuthScope.ANY_PORT), new UsernamePasswordCredentials(user, pwd));
			httpContext.setAttribute(ClientContext.CREDS_PROVIDER, provider);
		}
//		GraphStore graphStore = GraphStoreFactory.create() ;
		UpdateRequest request = UpdateFactory.create(query, Syntax.syntaxSPARQL_11);
		UpdateProcessor processor = UpdateExecutionFactory
			    .createRemoteForm(request, updateEndpoint);
			((UpdateProcessRemoteForm)processor).setHttpContext(httpContext);
			Long a = new Date().getTime();
			processor.execute();
		Long b = new Date().getTime();	
		return b-a;
	}
	
	public ResultSet returnSelect(){
		return rsSelect;
	}
	
	public long Update(){
		return updateTime;
	}
	
	public void start(){
		switch(method){
		case UPLOAD:
			updateTime = uploadFileIntern(file, graphURI, user, pwd, updateEndpoint);
			break;
		case LOAD:
			updateTime = loadUpdateIntern(filename, graphURI, user, pwd, updateEndpoint);
			break;
		case SELECT:
			rsSelect = selectIntern(query, queryTimeout, con);
			break;
		case UPDATE:
			updateTime = ownUpdate(query, user, pwd, updateEndpoint);
			break;
		case DELETE:
			try {
				updateTime = deleteFileIntern(file, graphURI, user, pwd, updateEndpoint);
			} catch (IOException e) {
				LogHandler.writeStackTrace(log, e, Level.SEVERE);
			}
			break;
		}
	}
	
	public void run() {
		start();
	}

	public ResultSet getRsSelect() {
		return rsSelect;
	}

	public Methods getMethod() {
		return method;
	}

	public Logger getLog() {
		return log;
	}

	public long getNumberOfTriples() {
		return numberOfTriples;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public String getFilename() {
		return filename;
	}

	public String getGraphURI() {
		return graphURI;
	}

	public String getUser() {
		return user;
	}

	public String getPwd() {
		return pwd;
	}

	public String getUpdateEndpoint() {
		return updateEndpoint;
	}

	public File getFile() {
		return file;
	}

	public String getQuery() {
		return query;
	}

	public int getQueryTimeout() {
		return queryTimeout;
	}

	public Connection getCon() {
		return con;
	}

	public void setMethod(Methods method) {
		this.method = method;
	}

	public void setLog(Logger log) {
		this.log = log;
	}

	public void setNumberOfTriples(long numberOfTriples) {
		this.numberOfTriples = numberOfTriples;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public void setGraphURI(String graphURI) {
		this.graphURI = graphURI;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public void setUpdateEndpoint(String updateEndpoint) {
		this.updateEndpoint = updateEndpoint;
	}

	public void setFile(File file) {
		this.file = file;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public void setQueryTimeout(int queryTimeout) {
		this.queryTimeout = queryTimeout;
	}

	public void setCon(Connection con) {
		this.con = con;
	}

	public Long getResTime() {
		return resTime;
	}

	public void setResTime(Long resTime) {
		this.resTime = resTime;
	}
}
