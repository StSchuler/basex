package org.basex.modules;

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.*;
import org.apache.lucene.facet.taxonomy.directory.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.*;
import org.apache.lucene.queryparser.classic.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import org.basex.core.*;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.query.value.node.*;
import org.basex.util.Token;

/**
 * Builds Lucene Index for current database context and provides
 * search functions.
 *
 * @author Stephan
 *
 */
public final class LuceneIndex {
  /**
   * Lucene Analyzer.
   */
  private static StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
  /**
   * Lucene Query.
   */
  private static Query baseQuery;
  /**
   * ArrayList containing Lucene search results.
   */
  private static ArrayList<Integer> resultContainer = new ArrayList<>();


  /**
   * Constructor.
   */
  private LuceneIndex() {

  }

  /**
   * Evaluates given Lucene Query and returns pre values
   * of found XML Nodes and facetresults.
   * @param data Database Data
   * @param drillDownField Drilldownfield
   * @throws Exception Exception
   */
  public static void drilldown(final Data data,
       final String... drillDownField) throws Exception {
    IOFile indexpath = data.meta.path;
    IndexReader reader = DirectoryReader.open(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex")));
    IndexSearcher searcher = new IndexSearcher(reader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));


    try {
      FacetsConfig fconfig = new FacetsConfig();
      fconfig.setHierarchical("text", true);

      //Query baseQuery = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);
      DrillDownQuery q = new DrillDownQuery(fconfig, baseQuery);

      q.add("text", drillDownField);


      /*
      //test drillSideways
      DrillSideways ds = new DrillSideways(searcher, fconfig, taxoReader);
      ScoreDoc[] hits1 = ds.search(q, 10).facets.;
      System.out.println(hits1);
      */


      FacetsCollector facetCollector = new FacetsCollector();
      ScoreDoc[] hits = null;

      hits = FacetsCollector.search(searcher, q, 1000000, facetCollector).scoreDocs;


      /*Facets facets = new FastTaxonomyFacetCounts(
          taxoReader, fconfig, facetCollector);
      FacetResult result = facets.getTopChildren(10, "text", drillDownField); //path???
       */

      ArrayList<Integer> pres = new ArrayList<>();
      for(int i = 0; i < hits.length; ++i) {
        int docId = hits[i].doc;
        Document d = searcher.doc(docId);
        Number a = d.getField("pre").numericValue();
        pres.add(data.pre((Integer) a));
      }

      resultContainer = pres;

    } finally {
      reader.close();
      taxoReader.close();
    }

    //resultContainer.add(result);
  }


  /**
   * Evaluates given Lucene Query and returns pre values
   * of found XML Nodes.
   * @param query Query
   * @param data Data
   * @throws ParseException Parse exception
   * @throws IOException I/O exception
   */
  public static void query(final String query, final Data data)
      throws ParseException, IOException {

      IOFile indexpath = data.meta.path;

      baseQuery = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);

      /*
      FacetsConfig fconfig = new FacetsConfig();
      //fconfig.setIndexFieldName("text", "$text");
      fconfig.setHierarchical("text", true);
      */

      IndexReader reader = DirectoryReader.open(FSDirectory.open(
          new File(indexpath.toString() + "/" + "LuceneIndex")));
      IndexSearcher searcher = new IndexSearcher(reader);
      /*
      TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
          new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));
      FacetsCollector facetsCollector = new FacetsCollector(true);
      */

      try {

        TopScoreDocCollector collector = TopScoreDocCollector.create(10, true);

        searcher.search(baseQuery, collector);
        ScoreDoc[] hits = collector.topDocs().scoreDocs;


        /*
        ScoreDoc[] hits = FacetsCollector.search(searcher, q, 1, facetsCollector).scoreDocs;
        Facets ftext = new FastTaxonomyFacetCounts(
             taxoReader, fconfig, facetsCollector);
         */

        /*
        FacetResult results = ftext.getTopChildren(10, "text");
         */

        ArrayList<Integer> pres1 = new ArrayList<>();
        for(int i = 0; i < hits.length; ++i) {
          int docId = hits[i].doc;
          Document d = searcher.doc(docId);
          Number a = d.getField("pre").numericValue();
          pres1.add(data.pre((Integer) a));
        }

        resultContainer = pres1;

      } finally {

        reader.close();
        //taxoReader.close();
      }


      //resultContainer.add(results);
  }

  /**
   * Get-Method for Lucene result container.
   * @return ArrayList containing Lucene search results.
   */
  public static ArrayList<Integer> getResults() {
    return resultContainer;
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

    File indexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex");
    File taxoIndexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex-taxo");
    indexFile.mkdir();

    FacetsConfig fconfig = new FacetsConfig();
    //fconfig.setIndexFieldName("text", "$text");
    fconfig.setHierarchical("text", true);

    Directory index = FSDirectory.open(indexFile);
    Directory taxoIndex = FSDirectory.open(taxoIndexFile);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
    IndexWriter writer = new IndexWriter(index, config);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoIndex, OpenMode.CREATE);

    try {
      Data data = context.data();
      int size = data.meta.size;

      for(int pre = 0; pre < size; pre++) {
        // reset output stream and serialize next item
        if(data.kind(pre) == Data.TEXT) {
          int parentpre = data.parent(pre, Data.TEXT);
          int parentid = data.id(parentpre);


          int pathpre = parentpre;
          ArrayList<String> path = new ArrayList<>();
          while(pathpre > 0) {
            System.out.println(pathpre);
            path.add(Token.string(data.name(pathpre, Data.ELEM)));
            pathpre = data.parent(pathpre, Data.ELEM);
          }

          //byte[] elem = data.name(parentpre, Data.ELEM);
          byte[] text = data.text(pre, true);

          Document doc = new Document();

          String[] pathArray = new String[path.size()];
          Collections.reverse(path);
          path.toArray(pathArray);

          doc.add(new IntField("pre", parentid, Field.Store.YES));
          doc.add(new TextField("text", Token.string(text), Field.Store.YES));
          doc.add(new FacetField("text", pathArray));
          writer.addDocument(fconfig.build(taxoWriter, doc));


        }
    }

      writer.forceMerge(5);
      writer.commit();
      taxoWriter.commit();

      //DBNode node = new DBNode(context.data(), 0);
      //Document doc = handler.getDocument(
      //new ByteArrayInputStream(node.serialize().toString().getBytes()));
      //writer.addDocument(doc);

    } finally {
      writer.close();
      taxoWriter.close();
    }


  }


  /**
   * Queries all facets of given Database and builds
   * XML structure for facet results.
   * @param data Data
   * @return ArrayList of ANode
   * @throws IOException I/O exception
   */
  public static ArrayList<ANode> facet(final Data data) throws IOException {
    IOFile indexpath = data.meta.path;

    FacetsConfig fconfig = new FacetsConfig();
    //fconfig.setIndexFieldName("text", "$text");
    fconfig.setHierarchical("text", true);

    IndexReader reader = DirectoryReader.open(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex")));
    IndexSearcher searcher = new IndexSearcher(reader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));
    FacetsCollector facetsCollector = new FacetsCollector(true);

    FacetsCollector.search(searcher, new MatchAllDocsQuery(), 1, facetsCollector);
    Facets ftext = new FastTaxonomyFacetCounts(
         taxoReader, fconfig, facetsCollector);

    FacetResult results = ftext.getTopChildren(10, "text");
    LabelAndValue[] labelValues = results.labelValues;

    ArrayList<ANode> facets = new ArrayList<>();

    for(int i = 0; i < labelValues.length; i++) {
      FElem elem = new FElem(labelValues[i].label).add(labelValues[i].value.toString());
      subElems(elem, ftext, labelValues[i].label);
      facets.add(elem);
    }

    return facets;
  }

  /**
   * Used for building hierarchical structure of XML nodes.
   * @param elem FElem
   * @param ftext Facets
   * @param path String...
   * @throws IOException I/OException
   */
  public static void subElems(final FElem elem, final Facets ftext, final String... path)
      throws IOException {
    FacetResult results = ftext.getTopChildren(10, "text", path);

    if(results != null) {
      LabelAndValue[] labelValues = results.labelValues;

      for(int i = 0; i < labelValues.length; i++) {
        FElem subElem = new FElem(labelValues[i].label).add(labelValues[i].value.toString());
        int length = path.length;
        String[] newPath = new String[length + 1];
        System.arraycopy(path, 0, newPath, 0, length);
        newPath[length] = labelValues[i].label;
        subElems(subElem, ftext, newPath);
        elem.add(subElem);
      }
    }

  }

}
