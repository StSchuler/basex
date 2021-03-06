package org.basex.query.func.fetch;

import static org.basex.query.util.Err.*;

import java.io.*;

import org.basex.io.*;
import org.basex.query.*;
import org.basex.query.func.*;
import org.basex.query.value.item.*;
import org.basex.util.*;

/**
 * Functions for fetching resources.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
public final class FNFetch extends StandardFunc {
  @Override
  public Item item(final QueryContext qc, final InputInfo ii) throws QueryException {
    switch(func) {
      case _FETCH_TEXT:         return text(qc);
      case _FETCH_BINARY:       return binary(qc);
      case _FETCH_CONTENT_TYPE: return contentType(qc);
      default:                  return super.item(qc, ii);
    }
  }

  /**
   * Fetches a resource identified by a URI and returns a string representation.
   * @param qc query context
   * @return string
   * @throws QueryException query exception
   */
  private StrStream text(final QueryContext qc) throws QueryException {
    final byte[] uri = toToken(exprs[0], qc);
    final String enc = toEncoding(1, BXFE_ENCODING_X, qc);
    return new StrStream(IO.get(Token.string(uri)), enc, BXFE_IO_X, qc);
  }

  /**
   * Fetches a resource identified by a URI and returns a binary representation.
   * @param qc query context
   * @return Base64Binary
   * @throws QueryException query exception
   */
  private B64Stream binary(final QueryContext qc) throws QueryException {
    final byte[] uri = toToken(exprs[0], qc);
    return new B64Stream(IO.get(Token.string(uri)), BXFE_IO_X);
  }

  /**
   * Fetches the content type of a resource.
   * @param qc query context
   * @return content type
   * @throws QueryException query exception
   */
  private Str contentType(final QueryContext qc) throws QueryException {
    final byte[] uri = toToken(exprs[0], qc);
    final IO io = IO.get(Token.string(uri));

    final String path = io.path();
    final String mt;
    if(io instanceof IOUrl) {
      try {
        mt = ((IOUrl) io).connection().getContentType();
      } catch(final IOException ex) {
        throw BXFE_IO_X.get(info, ex);
      }
    } else if(io instanceof IOContent) {
      mt = MimeTypes.APP_XML;
    } else {
      mt = io.exists() ? MimeTypes.get(path) : null;
    }
    if(mt == null) throw BXFE_IO_X.get(info, new FileNotFoundException(path));
    return Str.get(mt);
  }
}
