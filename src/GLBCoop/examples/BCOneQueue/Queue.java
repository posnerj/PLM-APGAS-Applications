/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoop.examples.BCOneQueue;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import GLBCoop.GLBResult;
import GLBCoop.TaskBag;
import GLBCoop.TaskQueue;
import GLBCoop.examples.BC.BC;
import java.io.Serializable;
import utils.MyIntegerDeque;
import utils.Rmat;

public class Queue extends BC implements TaskQueue<Queue, Double>, Serializable {

  private final MyIntegerDeque deque;

  private transient int s;
  private transient int state;

  public Queue(Rmat rmat, int permute, int numPlaces) {
    super(rmat, permute);
    this.s = 0;
    int max = places().size();
    int h = here().id;
    int lower = (int) ((long) (N) * h / max);
    int upper = (int) ((long) (N) * (h + 1) / max);
    int size = upper - lower;

    this.deque = new MyIntegerDeque(size);
    for (int i = 0; i < size; i++) {
      deque.offerLast(i + lower);
    }
  }

  /** substring helper function */
  public static String sub(String str, int start, int end) {
    return (str.substring(start, Math.min(end, str.length())));
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public void mergeResult(TaskQueue<Queue, Double> that) {
    mergeResult((Queue) that);
  }

  public void mergeResult(Queue that) {
    for (int i = 0; i < this.realBetweennessMap.length; i++) {
      this.realBetweennessMap[i] += that.realBetweennessMap[i];
    }
  }

  @Override
  public void printLog() {
    System.out.println(
        "[" + here().id + "]" + " Time = " + this.accTime + " Count = " + this.count);
  }

  public boolean process(int n) {
    for (int i = 0; i < n && size() > 0; ++i) {
      switch (state) {
        case 0:
          int u = deque.removeLast();
          refTime = System.nanoTime();
          s = this.verticesToWorkOn[u];
          this.state = 1;

        case 1:
          this.bfsShortestPath1(s);
          this.state = 2;

        case 2:
          while (!regularQueue.isEmpty()) {
            this.bfsShortestPath2();
          }
          this.state = 3;

        case 3:
          this.bfsShortestPath3();
          this.state = 4;

        case 4:
          while (!regularQueue.isEmpty()) {
            this.bfsShortestPath4(s);
          }
          this.accTime += ((System.nanoTime() - refTime) / 1E9);
          this.state = 0;
      }
    }
    return (size() > 0);
  }

  @Override
  public int size() {
    return this.deque.size();
  }

  @Override
  public TaskBag split() {
    int otherHalf = (int) (this.size() * 0.5);

    if (0 == otherHalf) {
      return null;
    }

    Bag bag = new Bag();
    bag.data = deque.getFromFirst(otherHalf);
    return bag;
  }

  @Override
  public void merge(TaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    deque.pushArrayFirst(bag.data);
  }

  @Override
  public GLBResult<Double> getResult() {
    return new BCGResult();
  }

  private class BCGResult extends GLBResult<Double> {

    @Override
    public Double[] getResult() {
      Double[] result = new Double[realBetweennessMap.length];
      for (int i = 0; i < realBetweennessMap.length; i++) {
        result[i] = realBetweennessMap[i];
      }
      return result;
    }

    @Override
    public void display(Double[] r) {
      for (int i = 0; i < N; ++i) {
        if (0.0 != r[i]) {
          System.out.println("(" + i + ") -> " + sub("" + r[i], 0, 6));
        }
      }
    }
  }
}
