/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.UTSOneQueue;

import static apgas.Constructs.here;

import FTGLB.FTGLBResult;
import FTGLB.FTTaskQueue;
import GLBCoop.TaskBag;
import GLBCoop.examples.UTSOneQueue.UTS;
import java.io.Serializable;

public final class Queue extends UTS implements FTTaskQueue<Queue, Long>, Serializable {

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
  public long count() {
    return this.count;
  }

  @Override
  public FTGLBResult<Long> getResult() {
    UTSResult result = new UTSResult();
    return result;
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  public void setResult(FTGLBResult<Long> r) {
    count = r.getResult()[0];
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(FTTaskQueue<Queue, Long> that) {
    if (that == null) {
      System.err.println(here() + "(in mergeResult): that is null");
    }
    count += that.count();
  }

  @Override
  public TaskBag split() {
    if (size() <= 1) {
      return null;
    }

    int otherHalf = (int) Math.max(this.size() * 0.1, 1);

    if (0 == otherHalf) {
      return null;
    }

    Bag bag = new Bag();
    bag.hash = super.getFromFirst(otherHalf);
    return bag;
  }

  @Override
  public void merge(TaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    super.pushArrayFirst(bag.hash);
  }

  @Override
  public void clearTasks() {
    clear();
  }

  @Override
  public TaskBag getAllTasks() {
    Bag bag = new Bag();
    bag.hash = super.peekFromFirst(size());
    return bag;
  }

  public class UTSResult extends FTGLBResult<Long> implements Serializable {

    private static final long serialVersionUID = 1L;

    long result;

    public UTSResult() {
      this.result = count;
    }

    public Long[] getResult() {
      return new Long[] {result};
    }

    @Override
    public void display(Long[] r) {
      System.out.println("Myresult: " + r[0]);
    }
  }
}
