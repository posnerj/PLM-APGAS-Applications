/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoop.examples.UTSOneQueue;

import static apgas.Constructs.here;

import GLBCoop.GLBResult;
import GLBCoop.TaskBag;
import GLBCoop.TaskQueue;
import java.io.Serializable;

public final class Queue extends UTS implements TaskQueue<Queue, Long>, Serializable {

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
  public GLBResult<Long> getResult() {
    UTSResult result = new UTSResult();
    return result;
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  public void setResult(GLBResult<Long> r) {
    count = r.getResult()[0];
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(TaskQueue<Queue, Long> that) {
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

    Bag bag = new Bag(otherHalf);
    bag.hash = super.getFromFirst(otherHalf);
    return bag;
  }

  @Override
  public void merge(TaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    super.pushArrayFirst(bag.hash);
  }

  public class UTSResult extends GLBResult<Long> implements Serializable {

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
