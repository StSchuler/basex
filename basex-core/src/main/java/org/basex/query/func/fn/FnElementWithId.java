package org.basex.query.func.fn;

import org.basex.query.*;
import org.basex.query.iter.*;

/**
 * Function implementation.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Christian Gruen
 */
public final class FnElementWithId extends Ids {
  @Override
  public Iter iter(final QueryContext qc) throws QueryException {
    final NodeSeqBuilder nc = new NodeSeqBuilder().check();
    add(ids(exprs[0].atomIter(qc, info)), nc, checkRoot(toNode(arg(1, qc), qc)), false);
    return nc;
  }
}
