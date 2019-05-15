/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTGLB.examples.NQueens;

import static apgas.Constructs.here;

import IncFTGLB.IncTaskBag;
import java.io.Serializable;
import java.util.Arrays;

/** Created by jposner on 05.07.17. */
public class Bag implements IncTaskBag, Serializable {

  public int[][] a;
  public int[] depth;

  public Bag(int size) {
    this.a = new int[size][];
    this.depth = new int[size];
  }

  @Override
  public int size() {
    return this.depth.length;
  }

  @Override
  public String toString() {
    return "Bag{ a=" + aToString() + ", depth=" + Arrays.toString(depth) + '}';
  }

  public String aToString() {
    String result = new String();
    for (int i = 0; i < this.depth.length; i++) {
      if (a[i] != null) {
        if (a[i].length > 0) {
          result += a[i][0] + " ";
        } else {
          result += "null ";
        }
      } else {
        result += "null ";
      }
    }
    return result;
  }

  @Override
  public void mergeAtTop(IncTaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    if ((null == bag) || (0 == bag.size())) {
      System.err.println(here() + " mergeAtTop: bag was empty!");
      return;
    }

    int bagSize = bag.size();
    int thisSize = this.size();
    int newSize = thisSize + bagSize;

    int[] depth = new int[newSize];
    System.arraycopy(this.depth, 0, depth, 0, thisSize);
    System.arraycopy(bag.depth, 0, depth, thisSize, bagSize);

    int[][] a = new int[newSize][];
    System.arraycopy(this.a, 0, a, 0, thisSize);
    System.arraycopy(bag.a, 0, a, thisSize, bagSize);

    this.depth = depth;
    this.a = a;
  }
}
