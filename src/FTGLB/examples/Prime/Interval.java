/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.Prime;

import java.io.Serializable;

public class Interval implements Serializable {

  private static final long serialVersionUID = 1L;

  public int min;
  public int max;

  public Interval(int min, int max) {
    this.min = min;
    this.max = max;
  }

  public Interval split() {
    Interval splitted = new Interval(min, min + (max - min) / 2);
    min = splitted.max + 1;
    return splitted;
  }

  public int size() {
    return max - min + 1;
  }
}
