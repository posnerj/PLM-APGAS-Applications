/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package utils;

import java.io.Serializable;
import java.lang.reflect.Array;

public class FixedRailQueue<T extends Serializable> implements Serializable {

  private final T[] internalStorage;

  private int head;
  private int tail;

  /** Construct a fixed size queue */
  @SuppressWarnings("unchecked")
  public FixedRailQueue(int n, Class<T> clazz) {
    this.internalStorage = (T[]) Array.newInstance(clazz, n);
    this.head = 0;
    this.tail = 0;
  }

  public static void main(String[] args) {
    FixedRailQueue<Integer> myQueue = new FixedRailQueue<>(4, Integer.class);

    myQueue.push(1);
    myQueue.push(2);
    myQueue.push(3);
    myQueue.push(4);
    myQueue.print();

    myQueue.top();
    myQueue.top();
    myQueue.print();

    myQueue.top();
    myQueue.top();
    myQueue.print();

    myQueue.push(5);
    myQueue.print();
  }

  /** Check if the queue is empty */
  public boolean isEmpty() {
    return this.head == this.tail;
  }

  /** Add the element to the front of the queue. */
  public void push(T t) {
    this.internalStorage[this.tail++] = t;
  }

  /** Output the contents of the queue in the order they are stored */
  public void print() {
    System.out.println("h = " + head + ", t = " + tail + ", ");
    System.out.print("[");
    for (int i = this.head; i < this.tail; ++i) {
      System.out.print(((i == this.head) ? "" : ",") + this.internalStorage[i]);
    }
    System.out.println("]");
  }

  /** Remove and return one element of the queue if FIFO order. */
  public T pop() {
    // Remove the first element from the queue.
    return this.internalStorage[this.head++];
  }

  /** Remove and return one element of the queue in LIFO order. */
  public T top() {
    return this.internalStorage[--this.tail];
  }

  /** Rewind. */
  public void rewind() {
    this.head = 0;
  }

  public int size() {
    return (this.tail - this.head);
  }
}
