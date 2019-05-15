/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoopGR.examples.BCOneQueue;

import GLBCoopGR.TaskBagGR;
import java.io.Serializable;

public class BagGR implements TaskBagGR, Serializable {

  public int[] data;

  @Override
  public int size() {
    return this.data.length;
  }
}
