/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoop.examples.NQueens;

import GLBCoop.TaskBag;
import java.util.Arrays;

/** Created by jposner on 05.07.17. */
public class Bag implements TaskBag {

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
    return "Bag{" + "a=" + aToString() + ", depth=" + Arrays.toString(depth) + '}';
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
}
