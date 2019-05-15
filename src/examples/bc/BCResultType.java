/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.bc;

import java.io.Serializable;

public class BCResultType implements Serializable {

  public final double[] internalResult;

  public BCResultType(int n) {
    this.internalResult = new double[n];
  }
}
