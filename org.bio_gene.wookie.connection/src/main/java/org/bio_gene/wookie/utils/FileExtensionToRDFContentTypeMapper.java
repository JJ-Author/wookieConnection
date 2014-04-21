package org.bio_gene.wookie.utils;

public class FileExtensionToRDFContentTypeMapper {
	
	
	public enum Extension{
		ttl, rdf, nt, n3 
	}
		
	public static String guessFileFormat(String contentType){
		switch(contentType){
		case "application/x-turtle":
			return "TURTLE";
		case "text/turtle":
			return "TURTLE";
		case "R":
			return "RDF/XML";
		case "text/n-triple":
			return "N-TRIPLE";
		case "":
			return "N3";
		default:
			return "PLAIN-TEXT";
		}
	}
	
	public static String guessFileFormat(Extension ext){
		switch(ext){
		case ttl: 
			return "TURTLE";
		case rdf:
			return "RDF/XML";
		case nt:
			return "N-TRIPLE";
		case n3:
			return "N3";
		default:
			return "TXT";
		}
	}
	
	
	public static String guessContentType(Extension ext){
		switch(ext){
			case ttl: 
				return "";
			case rdf:
				return "";
			case nt:
				return "";
			case n3:
				return "";
			default:
				return "";
		}
	}
	
	public static String guessFileExtension(String contentType){
		switch(contentType){
		case "application/x-turtle":
			return "ttl";
		case "text/turtle":
			return "ttl";
		case "R":
			return "rdf";
		case "text/n-triple":
			return "nt";
		case "":
			return "n3";
		default:
			return "txt";
			
		}
	}
	
	public static String guessFileExtensionFromFormat(String format){
		switch(format.toUpperCase()){
		case "TURTLE":
			return "ttl";
		case "RDF/XML":
			return "rdf";
		case "N-TRIPLE":
			return "nt";
		case "N3":
			return "n3";
		default:
			return "txt";
			
		}
	}
}
