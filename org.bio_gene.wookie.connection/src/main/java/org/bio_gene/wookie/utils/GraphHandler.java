package org.bio_gene.wookie.utils;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * Hilfsklasse um einen Graphen besser für SPARQL Strings zu handhaben
 * 
 * @author Felix Conrads
 *
 */
public class GraphHandler {

	public static void main(String[] argc){
		System.out.println(NodeFactory.createLiteral("bla asdkjahdsf"));
		System.out.println(NodeToSPARQLString(NodeFactory.createLiteral("bla asdkjahdsf")));
		System.out.println(NodeToSPARQLString(NodeFactory.createLiteral("bla \n asdkjahdsf")));
		System.out.println(NodeToSPARQLString(NodeFactory.createLiteral("bla asdkjahdsf")));
		System.out.println(NodeToSPARQLString(NodeFactory.createLiteral("bla \"?\"asdkjahdsf")));
		System.out.println(NodeFactory.createAnon().getBlankNodeLabel().replaceAll("[^a-zA-Z0-9]", ""));
	}
	
	/**
	 * Ermittelt zum Node ob es sich um eine URI, ein Literal 
	 * oder ein Blank Node handelt und gibt den entsprechenden String zurück:
	 * URI: \<uri\>
	 * Literal: "literal_wert"^^\<LiteralDatenTypURI\>
	 * Blank Node: Blank Node Label
	 * 
	 * @param n
	 * @return
	 */
	public static String NodeToSPARQLString(Node n){
		if(n.isURI()){
			return "<" + n + ">";
		}
		if(n.isLiteral()){
			String dataType = "";
			if(n.getLiteralDatatypeURI()!=null && n.getLiteralDatatypeURI().isEmpty()){		
				dataType += "^^<"+n.getLiteralDatatypeURI()+">";
			}
			n.getLiteral().toString(true);
			return n.getLiteral().toString(true)+dataType;
		}
		if(n.isBlank()){
			return "_:"+n.getBlankNodeLabel().replaceAll("[^a-zA-Z0-9]", "");
		}
		return n.toString();
	}
	
	/**
	 * Ermittelt zu einem Triple den SPARQLString in Form von:
	 * {s p o}
	 * Hierbei wird darauf geachtet was s, p ,o für ein Typ sind
	 * (s. NodeToSPARQLString)
	 * 
	 * @param t Triple zu dem der SPARQL String ermittelt werden soll
	 * @return SPARQL String zum Triple
	 */
	public static String TripleToSPARQLString(Triple t){
		String triple = "{";
		triple += NodeToSPARQLString(t.getSubject())+" ";
		triple += NodeToSPARQLString(t.getPredicate())+" ";
		triple += NodeToSPARQLString(t.getObject());
		triple +=" } ";
		return triple;
	}
	
	/**
	 * Ermittelt zu einem Graph den SPARQL String in Form von:
	 * { {s_1 p_1 o_1} {s_1 p_1 0_2} ... {s_n p_m o_t} }
	 * Es sind also alle Triple angegeben 
	 * 
	 * @param g Graph zu welchem der SPARQL String gesucht wird
	 * @return SPARQL String, welcher alle Triple enthält
	 */
	public static String GraphToSPARQLString(Graph g){
		ExtendedIterator<Triple> ti = GraphUtil.findAll(g);
		String ret="";
//		int i=0;
		while(ti.hasNext()){
			String triple = TripleToSPARQLString(ti.next());
			ret+= triple.substring(triple.indexOf("{")+1, triple.lastIndexOf("}"))+" . ";
//			i++;
		}
		ret = ret.substring(0,ret.lastIndexOf("."));
		
		ret = "{ "+ret+" }";
		

		return ret;
	}
	
}
