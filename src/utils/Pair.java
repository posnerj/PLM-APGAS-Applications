/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package utils;

import java.io.Serializable;

public class Pair<T extends Serializable, U extends Serializable> implements Serializable {

  private T first;
  private U second;

  public Pair(T first, U second) {
    this.first = first;
    this.second = second;
  }

  public T getFirst() {
    return first;
  }

  public void setFirst(T first) {
    this.first = first;
  }

  public U getSecond() {
    return second;
  }

  public void setSecond(U second) {
    this.second = second;
  }
}
