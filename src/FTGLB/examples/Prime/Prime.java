/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.Prime;

import java.io.Serializable;

// Calculates the prime-counting function using trial division.
public class Prime implements Serializable {

  private static final long serialVersionUID = 1L;

  protected Interval currentInterval;
  protected int count;
  protected int size;

  public Prime() {
    currentInterval = new Interval(0, -1);
  }

  public static void main(String[] args) {
    Prime prime = new Prime();
    prime.init(0, 1_000_000);

    long time = System.currentTimeMillis();
    while (prime.next()) {;
    }
    time = System.currentTimeMillis() - time;

    System.out.println("took " + time + " msecs with count = " + prime.count);
  }

  protected void init(int min, int max) {
    currentInterval = new Interval(min, max);
    size = currentInterval.size();
  }

  protected boolean next() {
    if (currentInterval.size() <= 0) {
      return false;
    }

    --size;
    final int n = currentInterval.min++;

    boolean prime = true;
    for (int i = 2; i < n; ++i) {
      if (n % i == 0) {
        prime = false;
        break;
      }
    }

    if (prime && n > 1) {
      ++count;
    }

    return true;
  }
}
