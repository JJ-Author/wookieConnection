package org.bio_gene.wookie.utils;

/**
 * Mapped RDF Files zu jeweiligen FileExtensions und umgekehrt
 * 
 * @author Felix Conrads
 *
 */
public class FileExtensionToRDFContentTypeMapper {
	
	/**
	 * Alle unterstützten Extensions
	 * 
	 * @author Felix Conrads
	 *
	 */
	public enum Extension{
		ttl, rdf, nt, n3 
	}

	/**
	 * Alle unterstützten Content-Types
	 * 
	 * @author Felix Conrads
	 *
	 */
	public enum ContentType{
		RDF, TURTLE, NT, N3
		
	}
		
	private static ContentType TranslateContentTypeToEnum(String contentType){
		switch(contentType){
		case "application/rdf+xml":
			return ContentType.RDF;
		case "application/x-turtle":
			return ContentType.TURTLE;
		case "text/turtle":
			return ContentType.TURTLE;
		case "text/plain":
			return ContentType.NT;
		case "text/rdf+n3":
			return ContentType.N3;
		default:
			return null;
		}
	}
	
	private static String TranslateEnumToContentType(ContentType contentType){
		switch(contentType){
		case RDF:
			return "application/rdf+xml";
		case TURTLE:
			return "application/x-turtle";
		case NT:
			return "text/plain";
		case N3:
			return "text/rdf+n3";
		default:
			return "text/plain";
		}
	}
	
	public static String guessFileFormat(String contentType){
		switch(TranslateContentTypeToEnum(contentType)){
		case TURTLE:
			return "TURTLE";
		case RDF:
			return "RDF/XML";
		case NT:
			return "N-TRIPLE";
		case N3:
			return "N3";
		default:
			return "PLAIN-TEXT";
		}
	}
	
	/**
	 * versucht zu ermitteln welches Format zur Extension
	 * zugehörig ist.
	 * Default: TXT
	 * 
	 * @param ext Die Extension zu welcher das Format ermittelt werden soll 
	 * @return Format zur Extension (default: txt)
	 */
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
	
	/**
	 * versucht zu ermitteln welches Content-Type zur Extension
	 * zugehörig ist.
	 * Default: text/plain
	 * 
	 * @param ext Die Extension zu welcher das Content-Type ermittelt werden soll 
	 * @return Content-Type zur Extension (default: text/plain)
	 */
	public static String guessContentType(Extension ext){
		switch(ext){
			case ttl: 
				return TranslateEnumToContentType(ContentType.TURTLE);
			case rdf:
				return TranslateEnumToContentType(ContentType.RDF);
			case nt:
				return TranslateEnumToContentType(ContentType.NT);
			case n3:
				return TranslateEnumToContentType(ContentType.N3);
			default:
				return "text/plain";
		}
		
	}
	
	/**
	 * versucht zu ermitteln welche Extension zum Content-Type
	 * zugehörig ist.
	 * Default: txt
	 * 
	 * @param ext Content-Type zu welcher die Extension ermittelt werden soll 
	 * @return Extension zur Content-Type (default: txt)
	 */
	public static String guessFileExtension(String contentType){
		switch(TranslateContentTypeToEnum(contentType)){
		case TURTLE:
			return "ttl";
		case RDF:
			return "rdf";
		case NT:
			return "nt";
		case N3:
			return "n3";
		default:
			return "txt";
			
		}
	}
	
	/**
	 * versucht zu ermitteln welche Extension zum Format 
	 * zugehörig ist.
	 * Default: txt
	 * 
	 * @param ext Format zu welcher die Extension ermittelt werden soll 
	 * @return Extension zum Format (default: txt)
	 */
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
