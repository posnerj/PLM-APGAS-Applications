/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar;

import java.io.Serializable;

public abstract class LocalFTGLBResult<T> implements Serializable {

  private static final long serialVersionUID = 1L;
  private T[] result;

  public LocalFTGLBResult() {
    this.result = null;
  }

  public abstract T[] getResult();

  public abstract void mergeResult(LocalFTGLBResult<T> other);

  public abstract void display(T[] param);
}
