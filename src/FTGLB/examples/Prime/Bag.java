/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.Prime;

import GLBCoop.TaskBag;
import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Bag implements TaskBag, Serializable {

  private static final long serialVersionUID = 1L;

  public Deque<Interval> intervals = new LinkedList<>();

  @Override
  public int size() {
    int size = 0;
    for (Interval interval : intervals) {
      size += interval.size();
    }
    return size;
  }
}
