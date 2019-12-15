/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.BCOneQueue;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import GLBCoop.examples.BC.BC;
import LocalFTTimedGLB_NoVar.LocalFTGLBResult;
import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import LocalFTTimedGLB_NoVar.LocalFTTaskQueue;
import java.io.Serializable;
import utils.MyIntegerDeque;
import utils.Rmat;

public class Queue extends BC implements LocalFTTaskQueue<Queue, Double>, Serializable {
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
  public void printLog() {
    System.out.println("[" + here() + "]" + " Count = " + count);
  }

  public boolean process(int n) {
    for (int i = 0; i < n && size() > 0; ++i) {
      switch (state) {
        case 0:
          int u = deque.removeLast();
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
  public LocalFTTaskBag split() {
    int otherHalf = (int) (this.size() * 0.5);

    if (0 == otherHalf) {
      return null;
    }

    Bag bag = new Bag();
    bag.data = deque.getFromFirst(otherHalf);
    return bag;
  }

  private class BCGResult extends LocalFTGLBResult<Double> {
    public final double[] result;

    public BCGResult() {
      this.result = realBetweennessMap.clone();
    }

    @Override
    public Double[] getResult() {
      Double[] r = new Double[result.length];
      for (int i = 0; i < result.length; ++i) {
        r[i] = this.result[i];
      }
      return r;
    }

    @Override
    public void display(Double[] r) {
      for (int i = 0; i < N; ++i) {
        if (0.0 != r[i]) {
          System.out.println("(" + i + ") -> " + sub("" + r[i], 0, 6));
        }
      }
    }

    @Override
    public void mergeResult(LocalFTGLBResult<Double> other) {
      final BCGResult otherResult = (BCGResult) other;
      for (int i = 0; i < this.result.length; i++) {
        this.result[i] += otherResult.result[i];
      }
    }
  }

  @Override
  public void merge(LocalFTTaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    deque.pushArrayFirst(bag.data);
  }

  @Override
  public LocalFTGLBResult<Double> getResult() {
    return new BCGResult();
  }

  @Override
  public LocalFTTaskBag getAllTasks() {
    Bag bag = new Bag();
    bag.data = deque.peekFromFirst(size());
    return bag;
  }

  @Override
  public void clearResult() {
    this.realBetweennessMap = new double[this.N];
  }
}
