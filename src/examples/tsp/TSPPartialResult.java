/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.tsp;

import java.io.Serializable;
import java.util.Arrays;

/** Created by dario on 7/31/17. */
public class TSPPartialResult implements Serializable {

  public int[] partialPath;
  public int sumOfWeights;
  public int count;
  public int countResults;

  public TSPPartialResult(int n) {
    partialPath = new int[n + 1];
    Arrays.fill(partialPath, 0);
    sumOfWeights = 0;
    countResults = 1;
    count = 0;
  }

  @Override
  public String toString() {

    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("Best path: ");
    for (int i = 0; i < partialPath.length; i++) {
      stringBuilder.append(partialPath[i]);
      if (i < partialPath.length - 1) {
        stringBuilder.append(" --> ");
      }
    }
    stringBuilder.append("\n");
    stringBuilder.append("Total path length: ");
    stringBuilder.append(sumOfWeights);
    stringBuilder.append("\n");
    stringBuilder.append("countResults: ");
    stringBuilder.append(countResults);
    return stringBuilder.toString();
  }
}
