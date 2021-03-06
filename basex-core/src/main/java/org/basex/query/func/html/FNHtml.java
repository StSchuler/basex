package org.basex.query.func.html;

import static org.basex.query.QueryText.*;
import static org.basex.query.util.Err.*;

import java.io.*;

import org.basex.build.*;
import org.basex.io.*;
import org.basex.query.*;
import org.basex.query.func.*;
import org.basex.query.value.item.*;
import org.basex.query.value.node.*;
import org.basex.util.*;

/**
 * Functions for converting HTML to XML.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
public final class FNHtml extends StandardFunc {
  /** QName. */
  private static final QNm Q_OPTIONS = QNm.get("options", HTMLURI);

  @Override
  public Item item(final QueryContext qc, final InputInfo ii) throws QueryException {
    switch(func) {
      case _HTML_PARSER: return Str.get(HtmlParser.parser());
      case _HTML_PARSE:  return parse(qc);
      default:           return super.item(qc, ii);
    }
  }

  /**
   * Converts HTML input to XML and returns a document node.
   * @param qc query context
   * @return document node
   * @throws QueryException query exception
   */
  private DBNode parse(final QueryContext qc) throws QueryException {
    final byte[] in = toBinary(exprs[0], qc);
    final HtmlOptions opts = toOptions(1, Q_OPTIONS, new HtmlOptions(), qc);
    try {
      return new DBNode(new HtmlParser(new IOContent(in), qc.context.options, opts));
    } catch(final IOException ex) {
      throw BXHL_IO_X.get(info, ex);
    }
  }
}
