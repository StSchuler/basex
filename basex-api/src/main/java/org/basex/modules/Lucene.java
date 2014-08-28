package org.basex.modules;

import java.io.*;


import org.apache.lucene.queryparser.classic.*;
import org.basex.query.*;
import org.basex.query.value.node.*;

/**
 * This module evaluates the lucene:search() command.
 *
 * @author Stephan
 *
 */
public class Lucene extends QueryModule{

  /**
   * Querys the given input String and returns
   * a collection of all found ANodes.
   * @param query Query
   * @return returns Array of found ANodes.
   */
  public ANode[] search(final String query) {
     int[] pres = null;
     try {
       pres = LuceneQuery.query(query, queryContext.context);
     } catch(ParseException e) {
       // TODO Auto-generated catch block
       e.printStackTrace();
     } catch(IOException e) {
       // TODO Auto-generated catch block
       e.printStackTrace();
     }

     ANode[] nodes = new ANode[pres.length];
     for(int i = 0; i < pres.length; i++) {
       DBNode n = new DBNode(queryContext.data(), pres[i]);
       nodes[i] = n;
     }
     return nodes;
   }


}
