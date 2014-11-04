package org.basex.query;

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.*;
import org.apache.lucene.facet.taxonomy.directory.*;
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
import org.basex.data.*;
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
   * @param data Data
   * @return int array of pre values
   * @throws ParseException Parse exception
   * @throws IOException I/O exception
   */
  public static int[] query(final String query, final Data data)
      throws ParseException, IOException {

      //IOFile indexpath = context.globalopts.dbpath();
      //String dbname = context.data().meta.name;
      IOFile indexpath = data.meta.path;
      //String dbname = data.meta.name;

      StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
      Query q = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);

      int hitsPerPage = 10;

      FacetsConfig fconfig = new FacetsConfig();
      fconfig.setMultiValued("text", true);
      fconfig.setIndexFieldName("text", "$text");

      IndexReader reader = DirectoryReader.open(FSDirectory.open(
          new File(indexpath.toString() + "/" + "LuceneIndex")));
      IndexSearcher searcher = new IndexSearcher(reader);
      TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
          new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));
      TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
      FacetsCollector facetsCollector = new FacetsCollector();

      FacetsCollector.search(searcher, q, 10, facetsCollector);
      Facets ftext = new TaxonomyFacetSumIntAssociations(
          "$text", taxoReader, fconfig, facetsCollector);

      List<FacetResult> results = new ArrayList<>();
      results.add(ftext.getTopChildren(10, "text"));

      System.out.println(results);

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
