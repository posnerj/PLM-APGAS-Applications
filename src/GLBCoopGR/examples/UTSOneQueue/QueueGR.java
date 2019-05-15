/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoopGR.examples.UTSOneQueue;

import static apgas.Constructs.here;

import GLBCoopGR.GLBResultGR;
import GLBCoopGR.TaskBagGR;
import GLBCoopGR.TaskQueueGR;
import java.io.Serializable;

public final class QueueGR extends UTS implements TaskQueueGR<QueueGR, Long>, Serializable {

  private static final long serialVersionUID = 1L;

  public QueueGR(int factor) {
    super(factor);
  }

  public QueueGR(int factor, int size) {
    super(factor, size);
  }

  public QueueGR(double den, int size) {
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
  public GLBResultGR<Long> getResult() {
    UTSResultGR result = new UTSResultGR();
    return result;
  }

  public void setResult(QueueGR q) {
    count = q.count;
  }

  public void setResult(GLBResultGR<Long> r) {
    count = r.getResult()[0];
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(TaskQueueGR<QueueGR, Long> that) {
    if (that == null) {
      System.err.println(here() + "(in mergeResult): that is null");
    }
    count += that.count();
  }

  @Override
  public TaskBagGR split() {
    if (size() <= 1) {
      return null;
    }

    int otherHalf = (int) Math.max(this.size() * 0.1, 1);

    if (0 == otherHalf) {
      return null;
    }

    BagGR bag = new BagGR(otherHalf);
    bag.hash = super.getFromFirst(otherHalf);
    return bag;
  }

  @Override
  public void merge(TaskBagGR taskBagGR) {
    BagGR bag = (BagGR) taskBagGR;
    super.pushArrayFirst(bag.hash);
  }

  public class UTSResultGR extends GLBResultGR<Long> implements Serializable {

    private static final long serialVersionUID = 1L;

    long result;

    public UTSResultGR() {
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
