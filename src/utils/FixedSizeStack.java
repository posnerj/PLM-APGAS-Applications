/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package utils;

import java.io.Serializable;
import java.lang.reflect.Array;

public class FixedSizeStack<T extends Serializable> implements Serializable {

  private final T[] data;
  private int size;

  @SuppressWarnings("unchecked")
  public FixedSizeStack(int size, Class<T> clazz) {
    this.data = (T[]) Array.newInstance(clazz, size);
    this.size = 0;
  }

  public synchronized T pop() {
    return this.data[--size];
  }

  public synchronized T get(int index) {
    return this.data[index];
  }

  public synchronized T push(T data) {
    return this.data[size++] = data;
  }

  public synchronized int getSize() {
    return size;
  }
}
