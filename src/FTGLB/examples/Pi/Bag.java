/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.Pi;

import GLBCoop.TaskBag;
import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Bag implements TaskBag, Serializable {

  public Deque<Integer> list = new LinkedList<>();

  @Override
  public int size() {
    return list.size();
  }
}
