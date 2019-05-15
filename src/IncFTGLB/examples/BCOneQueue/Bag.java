/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTGLB.examples.BCOneQueue;

import IncFTGLB.IncTaskBag;
import java.io.Serializable;

public class Bag implements IncTaskBag, Serializable {

  public int[] data;

  public void mergeAtTop(IncTaskBag that) {
    this.mergeAtTop((Bag) that);
  }

  public void mergeAtTop(Bag that) {
    int thisSize = this.size();
    int thatSize = that.size();
    int newSize = thisSize + thatSize;
    int[] newData = new int[newSize];

    System.arraycopy(this.data, 0, newData, 0, thisSize);
    System.arraycopy(that.data, 0, newData, thisSize, thatSize);

    this.data = newData;
  }

  @Override
  public int size() {
    return this.data.length;
  }
}
