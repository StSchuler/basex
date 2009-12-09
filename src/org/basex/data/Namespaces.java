package org.basex.data;

import static org.basex.core.Text.*;
import static org.basex.data.DataText.*;
import java.io.IOException;
import org.basex.io.DataInput;
import org.basex.io.DataOutput;
import org.basex.io.IO;
import org.basex.util.ObjectMap;
import org.basex.util.Token;
import org.basex.util.TokenSet;
import org.basex.util.Table;
import org.basex.util.TokenBuilder;
import org.basex.util.TokenList;

/**
 * This class contains the namespaces of a database.
 *
 * @author Workgroup DBIS, University of Konstanz 2005-09, ISC License
 * @author Christian Gruen
 */
public final class Namespaces {
  /** Namespace stack. */
  private final int[] uriStack = new int[IO.MAXHEIGHT];
  /** Current level. */
  private int uriL;
  /** Prefixes. */
  private final TokenSet pref;
  /** URIs. */
  private final TokenSet uri;
  /** Root node. */
  private NSNode root;
  /** Current node. */
  private NSNode tmp;

  // Building Namespaces ======================================================

  /**
   * Empty constructor.
   */
  public Namespaces() {
    root = new NSNode();
    root.pre = -1;
    pref = new TokenSet();
    uri = new TokenSet();
  }

  /**
   * Constructor, specifying an input stream.
   * @param in input stream
   * @throws IOException I/O exception
   */
  Namespaces(final DataInput in) throws IOException {
    pref = new TokenSet(in);
    uri = new TokenSet(in);
    root = new NSNode(in, null);
  }

  /**
   * Writes the namespaces to disk.
   * @param out output stream
   * @throws IOException I/O exception
   */
  void write(final DataOutput out) throws IOException {
    pref.write(out);
    uri.write(out);
    root.write(out);
  }

  /**
   * Adds the specified namespace to a temporary node.
   * @param p prefix
   * @param u uri
   */
  public void add(final byte[] p, final byte[] u) {
    if(tmp == null) tmp = new NSNode();
    final int k = addPref(p);
    final int v = addURI(u);
    tmp.add(k, v);
    if(p.length == 0) uriStack[uriL] = v;
  }

  /**
   * Completes the temporary node and adds it to the storage.
   * @param pre pre value
   * @return true if a new namespace has been added
   */
  public boolean open(final int pre) {
    final int us = uriStack[uriL];
    uriStack[++uriL] = us;
    if(tmp == null) return false;
    tmp.pre = pre;
    root.add(tmp);
    root = tmp;
    tmp = null;
    return true;
  }

  /**
   * Closes a node.
   * @param pre current pre value
   */
  public void close(final int pre) {
    while(root.pre >= pre) root = root.par;
    --uriL;
  }

  /**
   * Returns the namespace uri reference for the specified name,
   * or 0 if namespace cannot be found.
   * @param n tag/attribute name
   * @param elem element flag
   * @return namespace
   */
  public int uri(final byte[] n, final boolean elem) {
    if(uri.size() == 0) return 0;
    final byte[] pr = Token.pref(n);
    int u = elem ? uriStack[uriL] : 0;
    if(pr.length != 0) u = uri(pr, root);
    return u;
  }

  /**
   * Returns the number of uri references.
   * @return number of uri references
   */
  public int size() {
    return uri.size();
  }

  // Requesting Namespaces ====================================================

  /**
   * Returns the specified namespace uri.
   * @param id namespace uri reference
   * @return prefix
   */
  public byte[] uri(final int id) {
    return uri.key(id);
  }

  /**
   * Returns the namespace URI reference for the specified QName and pre value.
   * @param name tag/attribute name
   * @param pre pre value
   * @return namespace URI reference or 0 if no namespace was found
   */
  public int uri(final byte[] name, final int pre) {
    return uri(Token.pref(name), root.find(pre));
  }

  /**
   * Returns the specified prefix.
   * @param id prefix reference
   * @return prefix
   */
  byte[] pref(final int id) {
    return pref.key(id);
  }

  /**
   * Returns the prefix and URI references for the specified pre value.
   * @param pre pre value
   * @return namespace references
   */
  int[] get(final int pre) {
    return root.find(pre).vals;
  }

  /**
   * Returns the namespace URI reference for the specified prefix and node,
   * or 0 if namespace cannot be found.
   * @param pr prefix
   * @param node node to start with
   * @return namespace
   */
  private int uri(final byte[] pr, final NSNode node) {
    if(Token.eq(Token.XML, pr)) return 0;
    final int i = pref.id(pr);
    if(i == 0) return 0;

    NSNode nd = node;
    while(nd != null) {
      final int u = nd.uri(i);
      if(u != 0) return u;
      nd = nd.par;
    }
    return 0;
  }

  /**
   * Deletes the specified number of entries from the namespace structure.
   * @param pre pre value of the first node to delete
   * @param nr number of entries to be deleted
   */
  void delete(final int pre, final int nr) {
    NSNode nd = root.find(pre);
    if(nd.pre == pre) nd = nd.par;
    if(nd == null) root = new NSNode();

    while(nd != null) {
      nd.delete(pre, nr);
      nd = nd.par;
    }
  }

  /**
   * Adds a namespace for the specified pre value.
   * @param pre pre value
   * @param par parent
   * @param p prefix
   * @param u uri
   * @return uri reference
   */
  public int add(final int pre, final int par, final byte[] p, final byte[] u) {
    // [CG] ...to be merged with add()/open()/close() ?
    final NSNode nd = root.find(par);
    final NSNode t = new NSNode();
    t.pre = pre;

    final int k = addPref(p);
    final int v = addURI(u);
    t.add(k, v);
    nd.add(t);
    return v;
  }

  /**
   * Adds the specified namespace uri.
   * @param u namespace uri to be added
   * @return reference
   */
  public int addURI(final byte[] u) {
    return Math.abs(uri.add(u));
  }

  /**
   * Adds the specified prefix.
   * @param p prefix to be added
   * @return reference
   */
  public int addPref(final byte[] p) {
    return Math.abs(pref.add(p));
  }

  // Printing Namespaces ======================================================

  /**
   * Returns a tabular representation of the namespaces.
   * @param s start pre value
   * @param e end pre value
   * @return namespaces
   */
  public byte[] table(final int s, final int e) {
    if(root.ch.length == 0) return Token.EMPTY;

    final Table t = new Table();
    t.header.add(TABLEID);
    t.header.add(TABLEPRE);
    t.header.add(TABLEDIST);
    t.header.add(TABLEPREF);
    t.header.add(TABLEURI);
    for(int i = 0; i < 3; i++) t.align.add(true);
    table(t, root, s, e);
    return t.contents.size() != 0 ? t.finish() : Token.EMPTY;
  }

  /**
   * Adds the namespace structure for a node to the specified table.
   * @param t table
   * @param n namespace node
   * @param s start pre value
   * @param e end pre value
   */
  private void table(final Table t, final NSNode n, final int s, final int e) {
    for(int i = 0; i < n.vals.length; i += 2) {
      if(n.pre < s || n.pre > e) continue;
      final TokenList sl = new TokenList();
      sl.add(n.vals[i + 1]);
      sl.add(n.pre);
      sl.add(n.pre - n.par.pre);
      sl.add(pref.key(n.vals[i]));
      sl.add(uri.key(n.vals[i + 1]));
      t.contents.add(sl);
    }
    if(n.vals.length == 0) for(final NSNode c : n.ch) table(t, c, s, e);
  }

  /**
   * Returns namespace information.
   * @return info string
   */
  public byte[] info() {
    final ObjectMap<TokenList> map = new ObjectMap<TokenList>();
    info(map, root);
    final TokenBuilder tb = new TokenBuilder();
    for(final byte[] val : map.keys()) {
      tb.add("  ");
      final TokenList key = map.get(val);
      key.sort(false);
      final int ks = key.size();
      if(ks > 1 || key.get(0).length != 0) {
        if(key.size() != 1) tb.add("(");
        for(int k = 0; k < ks; k++) {
          if(k != 0) tb.add(", ");
          tb.add(key.get(k));
        }
        if(ks != 1) tb.add(")");
        tb.add(" = ");
      }
      tb.add("\"%\"" + NL, val);
    }
    return tb.finish();
  }

  /**
   * Adds namespace information for the specified node to a map.
   * @param map namespace map
   * @param n namespace node
   */
  private void info(final ObjectMap<TokenList> map, final NSNode n) {
    for(int i = 0; i < n.vals.length; i += 2) {
      final byte[] key = uri.key(n.vals[i + 1]);
      final byte[] val = pref.key(n.vals[i]);
      TokenList old = map.get(key);
      if(old == null) {
        old = new TokenList();
        map.put(key, old);
      }
      if(!old.contains(val)) old.add(val);
    }
    for(final NSNode c : n.ch) info(map, c);
  }

  /**
   * Returns a string representation of the namespaces.
   * @param s start pre value
   * @param e end pre value
   * @return string
   */
  public String toString(final int s, final int e) {
    return root.print(this, s - 1, e);
  }

  @Override
  public String toString() {
    return toString(0, Integer.MAX_VALUE);
  }
}
