/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package FTGLB.examples.SyntheticBenchmark;

import FTGLB.FTGLBResult;
import FTGLB.FTTaskQueue;
import GLBCoop.TaskBag;
import GLBCoop.examples.SyntheticBenchmark.Synthetic;
import java.io.Serializable;

public class Queue extends Synthetic implements FTTaskQueue<Queue, Long> {

  private static final long serialVersionUID = 5609090879416180904L;

  public Queue(int maxTaskChildren) {
    super(maxTaskChildren);
  }

  public Queue(int taskBallast, int taskCount, int taskGranularity) {
    super(taskBallast, taskCount, taskGranularity);
  }

  public static void main(String[] args) {
    final int taskCount = 1024 * 16;
    final int taskBallast = 1024 * 2;
    final int taskGranularity = 5;
    Queue queue = new Queue(taskBallast, taskCount, taskGranularity);
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
  public void clearTasks() {
    tasks.clear();
  }

  @Override
  public FTGLBResult<Long> getResult() {
    return new SyntheticResult(this.result);
  }

  @Override
  public void printLog() {}

  @Override
  public TaskBag getAllTasks() {
    SyntheticBag bag = new SyntheticBag(size());
    Object[] taskObjects = tasks.getFromFirst(size());
    System.arraycopy(taskObjects, 0, bag.tasks, 0, taskObjects.length);
    return bag;
  }

  @Override
  public TaskBag split() {
    int nStolen = Math.max(tasks.size() / 10, 1);
    if (tasks.size() < 2) {
      return null;
    }

    SyntheticBag taskBag = new SyntheticBag(nStolen);
    Object[] taskObjects = tasks.getFromFirst(nStolen);
    System.arraycopy(taskObjects, 0, taskBag.tasks, 0, taskObjects.length);

    return taskBag;
  }

  @Override
  public void mergeResult(FTTaskQueue<Queue, Long> that) {
    this.result += that.getResult().getResult()[0];
  }

  @Override
  public int size() {
    return tasks.size();
  }

  public void merge(TaskBag that) {
    SyntheticBag taskBag = (SyntheticBag) that;
    tasks.pushArrayFirst(taskBag.tasks);
  }

  public static class SyntheticResult extends FTGLBResult<Long> implements Serializable {

    private static final long serialVersionUID = 4173842626833583513L;

    private long result = 0;

    public SyntheticResult(long result) {
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
