package org.basex.modules;

import java.io.*;
import java.util.*;

import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Version;
import org.basex.data.*;
import org.basex.io.*;
import org.basex.query.*;
import org.basex.query.value.*;
import org.basex.query.value.item.*;
import org.basex.query.value.node.*;

/**
 * This module evaluates the lucene:search() command.
 *
 * @author Stephan
 *
 */
public class Lucene extends QueryModule{

  /**
   * Queries the given input String and returns
   * a collection of all found ANodes.
   * @param query Query
   * @param name String
   * @throws Exception exception
   */
  public void search(final String query, final String name) throws Exception {
     Data data = queryContext.resources.database(name, null);

     LuceneIndex.query(query, data);
     //ArrayList<Object>  resultContainer = LuceneIndex.query(query, data);
     //ArrayList<Integer> pres = (ArrayList<Integer>) resultContainer.get(0);
     //int length = ((ArrayList<Integer>) resultContainer.get(0)).size();

     //ANode[] nodes = new ANode[length];
     /*
     String facetResults = "<Facets>";
     LabelAndValue[] fvalues = ((FacetResult) resultContainer.get(1)).labelValues;

     for(int i = 0; i < fvalues.length; i++) {
       facetResults += "[" + fvalues[i] + "]";
     }

     facetResults += "</Facets>";
     ByteArrayInputStream input = new ByteArrayInputStream(facetResults.getBytes());
     IOStream io = new IOStream(input);
     DBNode fn = new DBNode(io);
     nodes[0] = fn;
     */

     /*
     for(int i = 0; i < length; i++) {
       int id =  pres.get(i);
       DBNode n = new DBNode(data, data.pre(id));
       nodes[i] = n;
     }
     return nodes;
     */
   }

  /**
   * Queries the given input String and returns
   * a collection of all found ANodes and drills
   * down on given element.
   * @param name Databasename
   * @param drillDownTerm Drilldownterm
   * @throws Exception exception
   */
  public void drillDown(final String name,
      final Value drillDownTerm) throws Exception {
    Data data = queryContext.resources.database(name, null);

    ArrayList<String> drill = new ArrayList<>();
    for(Item item : drillDownTerm) {
      drill.add((String) item.toJava());
    }
    String[] drillDownField = new String[drill.size()];
    drill.toArray(drillDownField);

    LuceneIndex.drilldown(data, drillDownField);
    //ArrayList<Object> resultContainer = LuceneIndex.drilldown(data, drillDownTerm);
    //ArrayList<Integer> pres = (ArrayList<Integer>) resultContainer.get(0);
    //int length = ((ArrayList<Integer>) resultContainer.get(0)).size();


    //ANode[] nodes = new ANode[length];
    /*
    String facetResults = "<Facets>";
    LabelAndValue[] fvalues = ((FacetResult) resultContainer.get(1)).labelValues;

    for(int i = 0; i < fvalues.length; i++) {
      facetResults += "[" + fvalues[i] + "]";
    }

    facetResults += "</Facets>";
    ByteArrayInputStream input = new ByteArrayInputStream(facetResults.getBytes());
    IOStream io = new IOStream(input);
    DBNode fn = new DBNode(io);
    nodes[0] = fn;
    */

    /*
    for(int i = 0; i < length; i++) {
      int id =  pres.get(i);
      DBNode n = new DBNode(data, data.pre(id));
      nodes[i] = n;
    }
    */
  }

  /**
   * Display Lucene search resuts of defined Query.
   * @param name Database name
   * @return Lucene sreach results
   * @throws QueryException Query exception
   */
  public ANode[] result(final String name) throws QueryException {
    Data data = queryContext.resources.database(name, null);

    ArrayList<Integer> resultContainer = LuceneIndex.getResults();
    int length = resultContainer.size();

    ANode[] nodes = new ANode[length];

    for(int i = 0; i < length; i++) {
      int id =  resultContainer.get(i);
      DBNode n = new DBNode(data, data.pre(id));
      nodes[i] = n;
    }
    return nodes;

  }

  /**
   * Merges Number Segments of the Index to
   * given Number maxNumSegments.
   * @param dbname Database Name
   * @param maxNumSeg Number of segments
   * @throws QueryException Query Exception
   * @throws IOException I/O Exception
   */
  public void optimize(final String dbname, final Int maxNumSeg)
      throws QueryException, IOException {

    int seg = (int) maxNumSeg.toJava();
    if(seg < 5) {
      StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
      Data data = queryContext.resources.database(dbname, null);
      IOFile indexpath = data.meta.path;
      File indexFile = new File(indexpath.toString() + "/" + "LuceneIndex");
      Directory index = FSDirectory.open(indexFile);
      IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
      IndexWriter writer = new IndexWriter(index, config);

      writer.forceMerge(seg);

      writer.close();
    }
  }

  /**
   * Returns facets of given Database.
   * @param name Database name
   * @return facets of given Database
   * @throws QueryException Query exception
   * @throws IOException  I/O exception
   */
  public ANode[] facets(final String name) throws QueryException, IOException {
    Data data = queryContext.resources.database(name, null);

    ArrayList<ANode> results = LuceneIndex.facet(data);
    int size = results.size();
    ANode[] resultNodes =  new ANode[size];

    results.toArray(resultNodes);

    return resultNodes;
  }

 /*
  /**
   * @param query
   * @param name
   * @return
   * @throws Exception
   */
  /*
  public ArrayList drilldown(final String name, final String query,
       final String drillDownField) throws Exception {
    InputInfo info = new InputInfo("test", 1, 1); //?
    Data data = queryContext.resources.database(name, info);
    IOFile indexpath = data.meta.path;
    IndexReader reader = DirectoryReader.open(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex")));
    IndexSearcher searcher = new IndexSearcher(reader);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(FSDirectory.open(
        new File(indexpath.toString() + "/" + "LuceneIndex-taxo")));

    FacetsConfig fconfig = new FacetsConfig();
    fconfig.setIndexFieldName("text", "$text");

    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
    Query baseQuery = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);
    DrillDownQuery q = new DrillDownQuery(fconfig, baseQuery);

    q.add("text", drillDownField);
    FacetsCollector facetCollector = new FacetsCollector();
    FacetsCollector.search(searcher, q, 10, facetCollector);

    Facets facets = new TaxonomyFacetSumIntAssociations(
        "$text", taxoReader, fconfig, facetCollector);
    FacetResult result = facets.getTopChildren(10, "text");
    System.out.println(result);

    List<MatchingDocs>  matchList = facetCollector.getMatchingDocs();
    Iterator<MatchingDocs> iter = matchList.iterator();

    ArrayList<Integer> pres = new ArrayList<>();
    while(iter.hasNext()) {
      MatchingDocs matchDocs = iter.next();
      DocIdSet a = matchDocs.bits;
      DocIdSetIterator docIter = a.iterator();

      int id = docIter.nextDoc();
      while(id != DocIdSetIterator.NO_MORE_DOCS) {
        Document doc = reader.document(id);
        Number b = doc.getField("pre").numericValue();
        pres.add((Integer) b);
        id = docIter.nextDoc();
      }
    }

    reader.close();
    taxoReader.close();

    ArrayList resultContainer = new ArrayList();
    resultContainer.add(pres);
    resultContainer.add(result);

    return resultContainer;
  }

   */
  /*
  /**
   * Evaluates given Lucene Query and returns pre values
   * of found XML Nodes.
   * @param query Query
   * @param data Data
   * @return int array of pre values
   * @throws ParseException Parse exception
   * @throws IOException I/O exception
   */
  /*
  public static ArrayList query(final String query, final Data data)
      throws ParseException, IOException {

      //IOFile indexpath = context.globalopts.dbpath();
      //String dbname = context.data().meta.name;
      IOFile indexpath = data.meta.path;
      //String dbname = data.meta.name;

      StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
      Query q = new QueryParser(Version.LUCENE_4_9, "text", analyzer).parse(query);

      int hitsPerPage = 10;

      FacetsConfig fconfig = new FacetsConfig();
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

      FacetResult results = ftext.getTopChildren(10, "text");
      LabelAndValue[] values = results.labelValues;

      for(int i = 0; i < values.length; i++) {
        System.out.println(values[i]);
      }

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

      ArrayList resultContainer = new ArrayList();
      resultContainer.add(pres);
      resultContainer.add(results);

      return resultContainer;
  }
  */

  /*
  /**
   * Builds luceneIndex of current database context.
   * @param context database context
   * @throws Exception exception
   */
  /*
  public static void luceneIndex(final Context context) throws Exception {

    //LuceneBuildIndex handler = new LuceneBuildIndex();
    IOFile indexpath = context.globalopts.dbpath();
    String dbname = context.data().meta.name;

    StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_9);
    File indexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex");
    File taxoIndexFile = new File(indexpath.toString() + "/" + dbname + "/" + "LuceneIndex-taxo");
    indexFile.mkdir();

    FacetsConfig fconfig = new FacetsConfig();
    fconfig.setIndexFieldName("text", "$text");

    Directory index = FSDirectory.open(indexFile);
    Directory taxoIndex = FSDirectory.open(taxoIndexFile);
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_9, analyzer);
    IndexWriter writer = new IndexWriter(index, config);
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoIndex, OpenMode.CREATE);


    Data data = context.data();
    int size = data.meta.size;

    for(int pre = 0; pre < size; pre++) {
      // reset output stream and serialize next item
      if(data.kind(pre) == Data.TEXT) {
        int parentpre = data.parent(pre, Data.TEXT);

        int pathpre = parentpre;
        ArrayList<String> path = new ArrayList<>();
        while(pathpre > 0) {
          System.out.println(pathpre);
          path.add(Token.string(data.name(pathpre, Data.ELEM)));
          pathpre = data.parent(pathpre, Data.ELEM);
        }

        byte[] elem = data.name(parentpre, Data.ELEM);
        byte[] text = data.text(pre, true);

        Document doc = new Document();

        String[] pathArray = new String[path.size()];
        path.toArray(pathArray);

        for(int i = 0; i < path.size(); i++) {
          System.out.println(pathArray[i]);
        }
        doc.add(new IntField("pre", parentpre, Field.Store.YES));
        doc.add(new TextField("text", Token.string(text), Field.Store.YES));
        doc.add(new IntAssociationFacetField(1, "text", Token.string(elem)));
        writer.addDocument(fconfig.build(taxoWriter, doc));
        //taxoWriter.addCategory(new FacetLabel("text", Token.string(elem)));
      }
    }

    writer.commit();
    taxoWriter.commit();

    //DBNode node = new DBNode(context.data(), 0);
    //Document doc = handler.getDocument(
    //new ByteArrayInputStream(node.serialize().toString().getBytes()));
    //writer.addDocument(doc);

    writer.close();
    taxoWriter.close();

  }
*/

}
