/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoopGR.examples.SyntheticBenchmark;

import GLBCoopGR.GLBResultGR;
import GLBCoopGR.TaskBagGR;
import GLBCoopGR.TaskQueueGR;
import java.io.Serializable;

public class QueueGR extends Synthetic implements TaskQueueGR<QueueGR, Long> {

  private static final long serialVersionUID = 5609090879416180904L;

  public QueueGR(int maxTaskChildren) {
    super(maxTaskChildren);
  }

  public QueueGR(int taskBallast, int taskCount, int taskGranularity) {
    super(taskBallast, taskCount, taskGranularity);
  }

  public static void main(String[] args) {
    final int taskCount = 1024 * 16;
    final int taskBallast = 1024 * 2;
    final int taskGranularity = 5;
    QueueGR queue = new QueueGR(taskBallast, taskCount, taskGranularity);
    long startTime = System.currentTimeMillis();
    queue.process(taskCount);
    long endTime = System.currentTimeMillis();
    System.out.println("Result: " + queue.getResult().getResult()[0]);
    System.out.println("Count: " + queue.count());
    System.out.println("Took " + (endTime - startTime) + " ms");
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public boolean process(int n) {
    int i = 0;
    for (; i < n && size() > 0; ++i) {
      calculate();
    }
    return size() > 0;
  }

  @Override
  public GLBResultGR<Long> getResult() {
    return new SyntheticResultGR(this.result);
  }

  @Override
  public void printLog() {}

  @Override
  public TaskBagGR split() {
    int nStolen = Math.max(tasks.size() / 10, 1);
    if (tasks.size() < 2) {
      return null;
    }

    SyntheticBagGR taskBag = new SyntheticBagGR(nStolen);
    Object[] taskObjects = tasks.getFromFirst(nStolen);
    System.arraycopy(taskObjects, 0, taskBag.tasks, 0, taskObjects.length);

    return taskBag;
  }

  @Override
  public void mergeResult(TaskQueueGR<QueueGR, Long> that) {
    this.result += that.getResult().getResult()[0];
  }

  @Override
  public int size() {
    return tasks.size();
  }

  public void merge(TaskBagGR that) {
    SyntheticBagGR taskBag = (SyntheticBagGR) that;
    tasks.pushArrayFirst(taskBag.tasks);
  }

  public static class SyntheticResultGR extends GLBResultGR<Long> implements Serializable {

    private static final long serialVersionUID = 4173842626833583513L;

    private long result = 0;

    public SyntheticResultGR(long result) {
      this.result = result;
    }

    @Override
    public Long[] getResult() {
      return new Long[] {result};
    }

    @Override
    public void display(Long[] param) {
      System.out.println("Myresult: " + param[0]);
    }
  }
}
