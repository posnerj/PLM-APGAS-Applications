/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTGLB.examples.PrimeOneQueue;

import IncFTGLB.IncTaskBag;
import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Bag implements IncTaskBag, Serializable {

  private static final long serialVersionUID = 1L;

  public Deque<Integer> deque = new LinkedList<>();

  @Override
  public int size() {
    return deque.size();
  }

  @Override
  public void mergeAtTop(IncTaskBag taskBag) {
    Bag bag = (Bag) taskBag;

    Integer[] array = bag.deque.toArray(new Integer[bag.size()]);
    for (int i = 0; i < bag.size(); ++i) {
      deque.addLast(array[i]);
    }
  }
}
