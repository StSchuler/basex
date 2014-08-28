package org.basex.query;

import java.io.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import org.basex.core.*;
import org.basex.io.*;


/**
 * Evaluates Lucene Querys.
 *
 * @author Stephan
 *
 */
public final class LuceneQuery{

  /**
   * Private constructor.
   */
  private LuceneQuery() {
    super();
  }

  /**
   * Evaluates given Lucene Query and returns pre values
   * of found XML Nodes.
   * @param query Query
   * @param context database context
   * @return int array of pre values
   * @throws ParseException Parse exception
   * @throws IOException I/O exception
   */
  public static int[] query(final String query, final Context context)
      throws ParseException, IOException {

      IOFile indexpath = context.globalopts.dbpath();
      String dbname = context.data().meta.name;

      StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
      Query q = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);

      int hitsPerPage = 10;


      IndexReader reader = DirectoryReader.open(FSDirectory.open(
          new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex")));
      IndexSearcher searcher = new IndexSearcher(reader);
      TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
      searcher.search(q, collector);
      ScoreDoc[] hits = collector.topDocs().scoreDocs;

      int[] pres = new int[hits.length];
      System.out.println("hits: " + hits.length);
      for(int i = 0; i < hits.length; ++i) {
        int docId = hits[i].doc;
        Document d = searcher.doc(docId);
        Number a = d.getField("pre").numericValue();
        pres[i] = (Integer) a;
      }

      reader.close();


      return pres;
  }

}
