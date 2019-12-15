/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package LocalFTTimedGLB_NoVar.examples.SyntheticBenchmark;

import GLBCoop.examples.SyntheticBenchmark.Synthetic;
import LocalFTTimedGLB_NoVar.LocalFTGLBResult;
import LocalFTTimedGLB_NoVar.LocalFTTaskBag;
import LocalFTTimedGLB_NoVar.LocalFTTaskQueue;
import java.io.Serializable;

public class Queue extends Synthetic implements LocalFTTaskQueue<Queue, Long> {
  private static final long serialVersionUID = 5609090879416180904L;

  public static void main(String[] args) {
    final int taskCount = 1024 * 16;
    final int taskBallast = 1024 * 2;
    final int taskGranularity = 5;
    Queue queue = new Queue(taskBallast, taskCount, taskGranularity);
    long startTime = System.currentTimeMillis();
    queue.process(taskCount);
    long endTime = System.currentTimeMillis();
    System.out.println("Result: " + queue.getResult().getResult()[0]);
    System.out.println("Took " + (endTime - startTime) + " ms");
  }

  public Queue(int maxTaskChildren) {
    super(maxTaskChildren);
  }

  public Queue(int taskBallast, int taskCount, int taskGranularity) {
    super(taskBallast, taskCount, taskGranularity);
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
  public LocalFTGLBResult<Long> getResult() {
    return new SyntheticResult(this.result);
  }

  @Override
  public void printLog() {}

  @Override
  public LocalFTTaskBag split() {
    int nStolen = Math.max(tasks.size() / 10, 1);
    if (tasks.size() < 2) return null;

    SyntheticBag taskBag = new SyntheticBag(nStolen);
    Object[] taskObjects = tasks.getFromFirst(nStolen);
    System.arraycopy(taskObjects, 0, taskBag.tasks, 0, taskObjects.length);

    return taskBag;
  }

  @Override
  public int size() {
    return tasks.size();
  }

  public void merge(LocalFTTaskBag that) {
    SyntheticBag taskBag = (SyntheticBag) that;
    tasks.pushArrayFirst(taskBag.tasks);
  }

  public static class SyntheticResult extends LocalFTGLBResult<Long> implements Serializable {
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

    @Override
    public void mergeResult(LocalFTGLBResult<Long> other) {
      this.result += other.getResult()[0];
    }
  }

  @Override
  public LocalFTTaskBag getAllTasks() {
    final int size = size();
    SyntheticBag taskBag = new SyntheticBag(size);
    Object[] taskObjects = tasks.peekFromFirst(size, 0);
    System.arraycopy(taskObjects, 0, taskBag.tasks, 0, size);
    return taskBag;
  }

  @Override
  public void clearResult() {
    this.result = 0;
  }
}
