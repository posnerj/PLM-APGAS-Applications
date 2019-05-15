/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.nqueens;

import apgas.impl.ResultAsyncAny;

/** Created by jposner on 15.03.17. */
public class NQueensResult extends ResultAsyncAny<Long> {

  public NQueensResult() {
    this.result = 0L;
  }

  public NQueensResult(Long r) {
    this.result = r;
  }

  @Override
  public void mergeResult(ResultAsyncAny<Long> r) {
    if (r == null) {
      return;
    }
    this.result += r.getResult();
  }

  @Override
  public void mergeResult(Long result) {
    this.result += result;
  }

  @Override
  public void display() {
    System.out.println("NQueens result: " + this.result);
  }

  @Override
  public String toString() {
    return Long.toString(this.result);
  }

  @Override
  public NQueensResult clone() {
    return new NQueensResult(this.result);
  }
}
