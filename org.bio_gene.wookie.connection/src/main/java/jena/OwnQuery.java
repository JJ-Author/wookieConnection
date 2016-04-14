package jena;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

public class OwnQuery extends Query {
	
	Query q;
	public static void main(String[] argc){
		String q = "SELECT * {?s ?p ?o} LIMIT 10";
		System.out.println(QueryFactory.create(q).serialize());
		System.out.println(new OwnQuery(QueryFactory.create(q)).serialize());
	}
	
	public OwnQuery(Query q){
		this.q=q;
	}
	
	@Override
	public String serialize(){
		return q.toString().replaceAll(" +", " ");
		
	}
	
	@Override
	public String toString(){
		return q.toString().replaceAll(" +", " ");
		
	}

}
