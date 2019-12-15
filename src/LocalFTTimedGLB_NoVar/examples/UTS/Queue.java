/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.UTS;

import GLBCoop.examples.UTS.UTS;
import LocalFTTimedGLB_NoVar.LocalFTGLBResult;
import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import LocalFTTimedGLB_NoVar.LocalFTTaskQueue;
import java.io.Serializable;

public final class Queue extends UTS implements LocalFTTaskQueue<Queue, Long>, Serializable {
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
  public LocalFTTaskBag split() {
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

  @Override
  public LocalFTTaskBag getAllTasks() {
    final int s = this.size();
    Bag bag = new Bag(s);
    System.arraycopy(this.hash, 0, bag.hash, 0, s);
    System.arraycopy(this.lower, 0, bag.lower, 0, s);
    System.arraycopy(this.upper, 0, bag.upper, 0, s);
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
  public void merge(LocalFTTaskBag taskBag) {
    this.merge((Bag) taskBag);
  }

  @Override
  public LocalFTGLBResult<Long> getResult() {
    UTSResult result = new UTSResult();
    return result;
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public int size() {
    return this.size;
  }

  public void setResult(LocalFTGLBResult<Long> r) {
    count = r.getResult()[0];
  }

  public void mergeResult(Queue q) {
    count += q.count;
  }

  public class UTSResult extends LocalFTGLBResult<Long> {
    private static final long serialVersionUID = 1L;

    Long[] result;

    public UTSResult() {
      this.result = new Long[] {count};
    }

    public Long[] getResult() {
      return result;
    }

    @Override
    public void display(Long[] r) {
      System.out.println("Myresult: " + r[0]);
    }

    @Override
    public void mergeResult(LocalFTGLBResult<Long> other) {
      final UTSResult otherResult = (UTSResult) other;
      this.result[0] += otherResult.getResult()[0];
    }
  }

  @Override
  public void clearResult() {
    this.count = 0;
  }
}
