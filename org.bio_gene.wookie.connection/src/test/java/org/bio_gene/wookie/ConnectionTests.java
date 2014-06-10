package org.bio_gene.wookie;


import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.bio_gene.wookie.connection.Connection;
import org.bio_gene.wookie.connection.ConnectionFactory;
import org.bio_gene.wookie.connection.CurlConnection;
import org.bio_gene.wookie.connection.ImplConnection;
import org.bio_gene.wookie.connection.Connection.UploadType;
import org.bio_gene.wookie.utils.ConfigParser;
import org.bio_gene.wookie.utils.GraphHandler;
import org.junit.Test;

import org.w3c.dom.Node;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;


public class ConnectionTests {
	
	@Test
	public void xmlTests(){
		Connection con = ConnectionFactory.createConnection("resources/config.xml");
		assertTrue(con instanceof ImplConnection);
		con.close();
		con = ConnectionFactory.createConnection("resources/config.xml", "curl");
		assertTrue(con instanceof CurlConnection);
		con.close();
//		con = ConnectionFactory.createConnection("resources/config.xml", "implcurl");
//		assertTrue(con instanceof ImplCurlConnection);
//		con.close();
		con = ConnectionFactory.createConnection("resources/config.xml", "impl");
		assertTrue(con instanceof ImplConnection);
		con.close();
	}

	@Test
	public void nodeTests(){
		Node db = null;
		try{
			ConfigParser cp = ConfigParser.getParser("resources/config.xml");
			cp.getElementAt("wookie", 0);
			db = (Node)cp.getElementAt("databases", 0);
		}
		catch(Exception e){
			assertTrue(false);
		}
		Connection con = ConnectionFactory.createConnection(db);
		assertTrue(con instanceof ImplConnection);
		con.close();
		con = ConnectionFactory.createConnection(db, "curl");
		assertTrue(con instanceof CurlConnection);
		con.close();
//		con = ConnectionFactory.createConnection(db, "implcurl");
//		assertTrue(con instanceof ImplCurlConnection);
//		con.close();
		con = ConnectionFactory.createConnection(db, "impl");
		assertTrue(con instanceof ImplConnection);
		con.close();
		
	}
	
	public Triple initFile(String fileName, String lang){
		com.hp.hpl.jena.graph.Node s = NodeFactory.createURI("http://bla.example.com");
		com.hp.hpl.jena.graph.Node p = NodeFactory.createURI("http://bla.example.com/#a");
		com.hp.hpl.jena.graph.Node o = NodeFactory.createLiteral(String.valueOf(Math.random()*100));
		Triple t = new Triple(s, p, o);
		Model m = ModelFactory.createDefaultModel();
		Graph g = m.getGraph();
		g.add(t);
		Model m1 = ModelFactory.createModelForGraph(g);
		File f = new File(fileName);
		try {
			f.createNewFile();
			m1.write(new FileOutputStream(f), lang);
		} catch (IOException e) {
			assertTrue(false);
		}
		return t;
	}
	
	public void sparqlTest(Connection con){
		con.setUploadType(UploadType.PUT);
		com.hp.hpl.jena.graph.Node s = NodeFactory.createURI("http://example.com");
		com.hp.hpl.jena.graph.Node p = NodeFactory.createURI("http://example.com/#");
		com.hp.hpl.jena.graph.Node o = NodeFactory.createLiteral("abc");
		Triple t = new Triple(s, p, o);
		//assertTrue(!selectTests(con, t));
//		dropTests(con);
		inputTests(con, t);
		assertTrue(selectTests(con, t));
		Triple t1 = initFile("file1.ttl", "TURTLE");
		fileTests(con, "file1.ttl");
		assertTrue(!selectTests(con, t));
		assertTrue(selectTests(con, t1));
		con.setUploadType(UploadType.POST);
		Triple t2 = initFile("file2.ttl", "TURTLE");
		fileTests(con, "file2.ttl");
		assertTrue(selectTests(con, t1));
		assertTrue(selectTests(con, t2));
		con.setUploadType(UploadType.PUT);
		inputTests(con, t);
		dropTests(con);
		assertTrue(!selectTests(con, t));
		assertTrue(!selectTests(con, t1));
		assertTrue(!selectTests(con, t2));
	}
	
	@Test
	public void connections(){
		implTest();
//		curlTest(); //only on Linux!
//		implCurlTest(); deprecated
	}
	
	public void curlTest(){
		Connection con = ConnectionFactory.createConnection("resources/config.xml", "curl");
		sparqlTest(con);
		con.close();
	}
	
	
	public void implCurlTest(){
		Connection con = ConnectionFactory.createConnection("resources/config.xml", "implcurl");
		sparqlTest(con);
		con.close();
	}
	
	
	public void implTest(){
		Connection con = ConnectionFactory.createConnection("resources/config.xml", "impl");
		sparqlTest(con);
		con.close();
	}


	public void inputTests(Connection con, Triple t){
		con.update("INSERT INTO <http://example.com> "
				+GraphHandler.TripleToSPARQLString(t));
		
	}
	

	public void fileTests(Connection con, String string){
		con.uploadFile(string, "http://example.com");
	}
	

	public void dropTests(Connection con){
		con.dropGraph("http://example.com");
	}
	
	
	public Boolean selectTests(Connection con, Triple t){
		try{
			ResultSet rs = con.select("SELECT ?s ?p ?o FROM <http://example.com> WHERE {?s ?p ?o} LIMIT 1");
			int i =0;
			while(rs.next()){
				if(! rs.getObject(1).toString().equals(t.getSubject().toString()))
					return false;
				if(! rs.getObject(2).toString().equals(t.getPredicate().toString()))
					return false;
//				assertEquals(rs.getObject(3).toString(), t.getObject().toString());
				i++;
			}
			if(i>0){
				return true;
			}
			return false;
		}catch(SQLException e){
			e.printStackTrace();
			assertTrue(false);
		}
		return null;
	}
}
