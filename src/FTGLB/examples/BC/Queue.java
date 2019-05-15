/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.BC;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import FTGLB.FTGLBResult;
import FTGLB.FTTaskQueue;
import GLBCoop.TaskBag;
import GLBCoop.examples.BC.BC;
import java.util.Arrays;
import utils.Rmat;

public class Queue extends BC implements FTTaskQueue<Queue, Double> {

  public int[] lower;
  public int[] upper;
  public transient int state = 0;
  public transient int s;
  protected int size;
  long[] receivedLids;
  long myLid;
  boolean done;

  public Queue(Rmat rmat, int permute, int numPlaces) {
    super(rmat, permute);
    this.lower = new int[4096];
    this.upper = new int[4096];
    final int h = here().id;
    final int max = places().size();
    this.lower[0] = (int) ((long) this.N * h / max);
    this.upper[0] = (int) ((long) this.N * (h + 1) / max);
    this.size = 1;
    this.receivedLids = new long[numPlaces];
    Arrays.setAll(this.receivedLids, i -> Long.MIN_VALUE); // i is the array index
    this.myLid = Long.MIN_VALUE;
    this.done = false;
  }

  public void grow() {
    int capacity = this.size * 2;
    int[] l = new int[capacity];
    System.arraycopy(this.lower, 0, l, 0, this.size);
    this.lower = l;
    int[] u = new int[capacity];
    System.arraycopy(this.upper, 0, u, 0, this.size);
    this.upper = u;
  }

  @Override
  public boolean process(int n) {
    int i = 0;
    int top = this.size - 1;
    int l = this.lower[top];
    int u = this.upper[top] - 1;

    switch (state) {
      case 0:
        refTime = System.nanoTime();
        s = this.verticesToWorkOn[u];
        this.state = 1;

      case 1:
        this.bfsShortestPath1(s);
        this.state = 2;

      case 2:
        while (!regularQueue.isEmpty()) {
          if (i++ > n) {
            return true;
          }
          this.bfsShortestPath2();
        }
        this.state = 3;

      case 3:
        this.bfsShortestPath3();
        this.state = 4;

      case 4:
        while (!regularQueue.isEmpty()) {
          if (i++ > n) {
            return true;
          }
          this.bfsShortestPath4(s);
        }
        this.accTime += ((System.nanoTime() - refTime) / 1e9);
        this.state = 0;
        if (u == l) {
          --this.size;
        } else {
          this.upper[top] = u;
        }
    }
    return (0 < this.size);
  }

  @Override
  public TaskBag split() {
    int s = 0;
    for (int i = 0; i < this.size; ++i) {
      if (2 <= (this.upper[i] - this.lower[i])) {
        ++s;
      }
    }

    if (s == 0) {
      return null;
    }

    Bag bag = new Bag(s);
    s = 0;
    for (int i = 0; i < this.size; i++) {
      int p = this.upper[i] - this.lower[i];
      if (2 <= p) {
        // bag.upper(s) = upper(i);
        // upper(i) -= (p / 2n);
        // bag.lower(s++) = upper(i);
        bag.lower[s] = this.lower[i];
        bag.upper[s] = this.upper[i] - ((p + 1) / 2);
        this.lower[i] = bag.upper[s++];
      }
    }
    return (bag);
  }

  public void merge(Bag bag) {
    int bagSize = bag.size();
    int thisSize = this.size;
    while (this.upper.length < bagSize + thisSize) {
      grow();
    }
    System.arraycopy(this.lower, 0, this.lower, bagSize, thisSize);
    System.arraycopy(this.upper, 0, this.upper, bagSize, thisSize);

    System.arraycopy(bag.lower, 0, this.lower, 0, bagSize);
    System.arraycopy(bag.upper, 0, this.upper, 0, bagSize);
    this.size += bagSize;
  }

  @Override
  public void merge(TaskBag taskBag) {
    this.merge((Bag) taskBag);
  }

  public void merge(FTTaskQueue<Queue, Double> other) {
    this.merge((Queue) other);
  }

  public void merge(Queue other) {
    int thisSize = this.size();
    int otherSize = (null == other ? 0 : other.size());
    if (0 == otherSize) {
      return;
    }

    while (thisSize + otherSize > this.lower.length) {
      this.grow();
    }

    System.arraycopy(other.lower, 0, this.lower, thisSize, otherSize);
    System.arraycopy(other.upper, 0, this.upper, thisSize, otherSize);
    this.size += otherSize;
  }

  @Override
  public void mergeResult(FTTaskQueue<Queue, Double> other) {
    this.mergeResult((Queue) other);
  }

  public void mergeResult(Queue other) {
    for (int i = 0; i < other.realBetweennessMap.length; ++i) {
      this.realBetweennessMap[i] += other.realBetweennessMap[i];
    }
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public void clearTasks() {
    this.lower = new int[4096];
    this.upper = new int[4096];
    this.size = 0;
  }

  @Override
  public TaskBag getAllTasks() {
    final int s = this.size();
    Bag bag = new Bag(s);
    System.arraycopy(this.lower, 0, bag.lower, 0, s);
    System.arraycopy(this.upper, 0, bag.upper, 0, s);
    return bag;
  }

  @Override
  public void printLog() {
    System.out.println(
        "[" + here().id + "]" + " Time = " + this.accTime + " Count = " + this.count);
  }

  @Override
  public FTGLBResult<Double> getResult() {
    BCGResult result = new BCGResult();
    return result;
  }

  public class BCGResult extends FTGLBResult<Double> {

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
