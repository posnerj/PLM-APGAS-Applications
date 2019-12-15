/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.UTSOneQueue;

import GLBCoop.examples.UTSOneQueue.UTS;
import LocalFTTimedGLB_NoVar.LocalFTGLBResult;
import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import LocalFTTimedGLB_NoVar.LocalFTTaskQueue;
import java.io.Serializable;

public final class Queue extends UTS implements LocalFTTaskQueue<Queue, Long>, Serializable {
  private static final long serialVersionUID = 1L;

  public Queue(int factor) {
    super(factor);
  }

  public Queue(int factor, int size) {
    super(factor, size);
  }

  public Queue(double den, int size) {
    super(den, size);
  }

  @Override
  public boolean process(int n) {
    int i = 0;
    for (; i < n && size() > 0; ++i) {
      this.expand();
    }
    count += i;
    return size() > 0;
  }

  @Override
  public LocalFTGLBResult<Long> getResult() {
    UTSResult result = new UTSResult();
    return result;
  }

  public void setResult(LocalFTGLBResult<Long> r) {
    count = r.getResult()[0];
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  @Override
  public void printLog() {
    return;
  }

  public class UTSResult extends LocalFTGLBResult<Long> implements Serializable {
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
      UTSResult result = (UTSResult) other;
      this.result[0] += result.getResult()[0];
    }
  }

  @Override
  public LocalFTTaskBag split() {
    if (size() <= 1) return null;

    int otherHalf = (int) Math.max(this.size() * 0.1, 1);

    if (0 == otherHalf) {
      return null;
    }

    Bag bag = new Bag();
    bag.hash = super.getFromFirst(otherHalf);
    return bag;
  }

  @Override
  public void merge(LocalFTTaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    super.pushArrayFirst(bag.hash);
  }

  @Override
  public LocalFTTaskBag getAllTasks() {
    Bag bag = new Bag();
    bag.hash = super.peekFromFirst(size());
    return bag;
  }

  @Override
  public void clearResult() {
    this.count = 0;
  }
}
