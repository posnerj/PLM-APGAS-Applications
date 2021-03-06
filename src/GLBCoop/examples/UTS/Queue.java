/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoop.examples.UTS;

import static apgas.Constructs.here;

import GLBCoop.GLBResult;
import GLBCoop.TaskBag;
import GLBCoop.TaskQueue;
import java.io.Serializable;

public final class Queue extends UTS implements TaskQueue<Queue, Long>, Serializable {

  private static final long serialVersionUID = 1L;

  UTSResult result = null;

  public Queue(int factor, int numPlaces) {
    super(factor);
  }

  @Override
  public boolean process(int n) {
    int i = 0;
    for (; ((i < n) && (this.size() > 0)); ++i) {
      this.expand();
    }
    count += i;
    return (this.size() > 0);
  }

  @Override
  public TaskBag split() {
    int s = 0;
    for (int i = 0; i < size; ++i) {
      if ((this.upper[i] - this.lower[i]) >= 2) {
        ++s;
      }
    }

    if (s == 0) {
      return null;
    }

    Bag bag = new Bag(s);
    s = 0;
    for (int i = 0; i < this.size; ++i) {
      int p = this.upper[i] - this.lower[i];
      if (p >= 2) {
        bag.hash[s] = this.hash[i];
        bag.upper[s] = this.upper[i];
        this.upper[i] -= p / 2;
        bag.lower[s++] = this.upper[i];
      }
    }
    return bag;
  }

  public void merge(Bag bag) {
    int bagSize = (null == bag ? 0 : bag.size());

    if (bagSize == 0) {
      return;
    }

    int thisSize = this.size();

    while (bagSize + thisSize > this.hash.length) {
      grow();
    }

    System.arraycopy(bag.hash, 0, this.hash, thisSize, bagSize);
    System.arraycopy(bag.lower, 0, this.lower, thisSize, bagSize);
    System.arraycopy(bag.upper, 0, this.upper, thisSize, bagSize);
    this.size += bagSize;
  }

  @Override
  public void merge(TaskBag taskBag) {
    this.merge((Bag) taskBag);
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public GLBResult<Long> getResult() {
    UTSResult result = new UTSResult();
    return result;
  }

  public void setResult(GLBResult<Long> r) {
    count = r.getResult()[0];
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(TaskQueue<Queue, Long> other) {
    if (other == null) {
      System.err.println(here() + "(in mergeResult): other is null");
    }
    count += other.count();
  }

  @Override
  public int size() {
    return this.size;
  }

  public void mergeResult(Queue q) {
    count += q.count;
  }

  public class UTSResult extends GLBResult<Long> {

    private static final long serialVersionUID = 1L;

    Long[] result;

    public UTSResult() {
      this.result = new Long[1];
    }

    public Long[] getResult() {
      result[0] = count;
      return result;
    }

    @Override
    public void display(Long[] r) {
      System.out.println("Myresult: " + r[0]);
    }
  }
}
