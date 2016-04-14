package org.bio_gene.wookie;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.jena.graph.Graph;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.bio_gene.wookie.utils.FileExtensionToRDFContentTypeMapper;
import org.bio_gene.wookie.utils.GraphHandler;
import org.bio_gene.wookie.utils.LogHandler;
import org.junit.Test;



public class UtilityTests {


    public Graph initGraph(){
        Model m = ModelFactory.createDefaultModel();
        Literal lit = m.createLiteral("asd");
        Property prop = m.createProperty("http://example.com/asd");
        Literal i = m.createTypedLiteral(1);

        m.createResource("http://example.com")
        .addProperty(prop, "asdc").addLiteral(prop, lit)
        .addLiteral(prop, i);
        Graph g = m.getGraph();
        return g;
    }

    @Test
    public void graphHandlerTest(){
        Graph g = initGraph();
        assertTrue(g.size()==3);
        assertEquals(GraphHandler.GraphToSPARQLString(g),
                "{{<http://example.com> <http://example.com/asd> "
                        + "\"1\"^^<http://www.w3.org/2001/XMLSchema#int>}{<http://example.com> <http://example.com/asd> "
                        + "\"asd\"}{<http://example.com> <http://example.com/asd> \"asdc\"}}");
    }

    @Test
    public void mapperTest(){
        String[] ext = {"ttl", "rdf", "as", "nt", "n3"};
        String[] format = {"TURTLE",  "RDF/XML", "as", "N-TRIPLE",  "N3"};
        String[] cot = {"application/x-turtle", "application/rdf+xml",
                "as", "text/plain", "text/rdf+n3"};
        String ct, ex1, ex2, fo, fo2;
        for(int i=0; i<ext.length;i++){
            try{
            ct = FileExtensionToRDFContentTypeMapper.guessContentType(
                    FileExtensionToRDFContentTypeMapper.Extension.valueOf(ext[i]));
            fo = FileExtensionToRDFContentTypeMapper.guessFileFormat(cot[i]);
            fo2 = FileExtensionToRDFContentTypeMapper.guessFileFormat(
                    FileExtensionToRDFContentTypeMapper.Extension.valueOf(ext[i]));
            ex1 = FileExtensionToRDFContentTypeMapper.guessFileExtensionFromFormat(format[i]);
            ex2 = FileExtensionToRDFContentTypeMapper.guessFileExtension(cot[i]);
            assertEquals(fo, fo2);
            assertEquals(ex1, ex2);
            switch(ext[i]){

            case("ttl"):
                assertEquals(ct, "application/x-turtle");
                assertEquals(fo2, "TURTLE");
                assertEquals(ex2, "ttl");
                break;
            case("rdf"):
                assertEquals(ct, "application/rdf+xml");
                assertEquals(fo2, "RDF/XML");
                assertEquals(ex2, "rdf");
                break;
            case("nt"):
                assertEquals(ct, "text/plain");
                assertEquals(fo2, "N-TRIPLE");
                break;
            case("n3"):
                assertEquals(ct, "text/rdf+n3");
                assertEquals(fo2, "N3");
                assertEquals(ex2, "n3");
                break;
            case("as"):
                assertEquals(ct, "text/plain");
                assertEquals(fo2, "PLAIN-TEXT");
                assertEquals(ex2, "txt");
                break;
            }
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    @Test
    public void logTest(){

        Logger log = Logger.getLogger(this.getClass().getName());
        assertNotNull(log);
        log.setLevel(Level.FINEST);
        LogHandler.initLogFileHandler(log, "test");
        File file = new File("logs/test+.log.0");
        assertTrue(file.exists());
        log.warning("test2");
        log.info("Test");
        log.finest("test2");
        try{
            throw new NullPointerException();
        }
        catch(Exception e){
            LogHandler.writeStackTrace(log, e, Level.SEVERE);
        }
        Long fileLength = file.length();
        assertTrue(fileLength > 0L);
        log.warning("test2");
    }



}
