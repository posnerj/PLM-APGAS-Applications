/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB;

import java.io.Serializable;

public abstract class FTGLBResult<T extends Serializable> implements Serializable {

  private static final long serialVersionUID = 1L;
  int op;
  private T[] result;

  public FTGLBResult() {
    this.result = null;
    this.op = -1;
  }

  public abstract T[] getResult();

  public void setResult(T[] result) {
    this.result = result;
  }

  public abstract void display(T[] param);

  public T[] submitResult() {
    if (this.result == null) {
      this.result = getResult();
    }
    return this.result;
  }
}
