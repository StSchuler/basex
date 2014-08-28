package org.basex.index;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.basex.core.Context;
import org.basex.data.Data;
import org.basex.io.*;
import org.basex.util.Token;



/**
 * Builds Lucene Index for current database context.
 *
 * @author Stephan
 *
 */
public class LuceneBuildIndex extends DefaultHandler {
  /** Element Buffer.  */
  private StringBuilder elementBuffer = new StringBuilder();
  /** Attribut Map.  */
  private Map<String, String> attributeMap = new HashMap<>();
  /** Lucene Document.  */
  private Document doc;

  /**
   * Parses InputStream of XML document to Lucene Document.
   * @param is InputStream
   * @return Lucene Document
   * @throws Exception Exception
   */
  public Document getDocument(final InputStream is)
      throws Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    try {
      SAXParser parser = spf.newSAXParser();
      parser.parse(is, this);
    } catch (Exception e) {
      throw new Exception(
          "Cannot parse XML document", e);
    }
    return doc;
  }

  @Override
  public void startDocument() {
    doc = new Document();
  }

  @Override
  public void startElement(final String uri, final String localName,
      final String qName, final Attributes atts)
      throws SAXException {
    elementBuffer.setLength(0);
    attributeMap.clear();
    int numAtts = atts.getLength();
    if (numAtts > 0) {
      for (int i = 0; i < numAtts; i++) {
        attributeMap.put(atts.getQName(i), atts.getValue(i));
            }
        }
    }


  @Override
  public void characters(final char[] text, final int start, final int length) {
    elementBuffer.append(text, start, length);
  }


  @Override
  public void endElement(final String uri, final String localName, final String qName)
      throws SAXException {
    doc.add(new TextField(qName, elementBuffer.toString(), Field.Store.YES));

  }

  /**
   * Builds luceneIndex of current database context.
   * @param context database context
   * @throws Exception exception
   */
  public static void luceneIndex(final Context context) throws Exception {

    //LuceneBuildIndex handler = new LuceneBuildIndex();
    IOFile indexpath = context.globalopts.dbpath();
    String dbname = context.data().meta.name;

    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
    File indexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex");
    indexFile.mkdir();

    Directory index = FSDirectory.open(indexFile);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
    IndexWriter writer = new IndexWriter(index, config);


    Data data = context.data();
    int size = data.meta.size;

    for(int pre = 0; pre < size; pre++) {
      // reset output stream and serialize next item
      if(data.kind(pre) == Data.TEXT) {
        //int parentpre = data.parent(pre, Data.TEXT);
        //byte[] elem = data.name(parentpre, Data.ELEM);
        byte[] text = data.text(pre, true);

        Document doc = new Document();
        doc.add(new IntField("pre", pre, Field.Store.YES));
        doc.add(new TextField("text", Token.string(text), Field.Store.YES));
        writer.addDocument(doc);
      }
    }


    //DBNode node = new DBNode(context.data(), 0);
    //Document doc = handler.getDocument(
    //new ByteArrayInputStream(node.serialize().toString().getBytes()));
    //writer.addDocument(doc);

    writer.close();

  }

}