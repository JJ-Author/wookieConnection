package org.bio_gene.wookie.utils;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class GraphHandler {

	
	public static String NodeToSPARQLString(Node n){
		return (String) (n.isURI() ? ("<" + n + ">"): (n.isLiteral() ? ("\""+ n.getLiteralValue() + "\"" + 
				(n.getLiteralDatatypeURI() != null ? ("^^<"+ n.getLiteralDatatype().getURI() + ">") : "")) : n.isBlank() ? n.getBlankNodeLabel() : n));
	}
	
	public static String TripleToSPARQLString(Triple t){
		String triple = "{";
		triple += NodeToSPARQLString(t.getSubject())+" ";
		triple += NodeToSPARQLString(t.getPredicate())+" ";
		triple += NodeToSPARQLString(t.getObject());
		triple +="}";
		return triple;
	}
	
	public static String GraphToSPARQLString(Graph g){
		ExtendedIterator<Triple> ti = GraphUtil.findAll(g);
		String ret = "{";
		while(ti.hasNext()){
			ret+=TripleToSPARQLString(ti.next());
		}
		ret += "}";
		return ret;
	}
	
}
