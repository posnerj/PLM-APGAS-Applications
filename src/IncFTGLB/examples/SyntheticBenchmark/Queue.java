/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTGLB.examples.SyntheticBenchmark;

import GLBCoop.examples.SyntheticBenchmark.Synthetic;
import IncFTGLB.IncFTGLBResult;
import IncFTGLB.IncFTTaskQueue;
import IncFTGLB.IncTaskBag;
import java.io.Serializable;

public class Queue extends Synthetic implements IncFTTaskQueue<Queue, Long> {

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
    for (int i = 0; i < taskCount; ++i) {
      queue.process();
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Result: " + queue.getResult().getResult()[0]);
    System.out.println("Count: " + queue.count());
    System.out.println("Took " + (endTime - startTime) + " ms");
  }

  @Override
  public void process() {
    calculate();
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public void setCount(long count) {
    this.count = count;
  }

  @Override
  public IncFTGLBResult<Long> getResult() {
    return new SyntheticResult(this.result);
  }

  @Override
  public void setResult(IncFTGLBResult<Long> result) {
    this.result = result.getResult()[0];
  }

  @Override
  public void printLog() {}

  @Override
  public IncTaskBag split() {
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
  public void mergeResult(IncFTTaskQueue<Queue, Long> that) {
    this.result += that.getResult().getResult()[0];
  }

  @Override
  public int size() {
    return tasks.size();
  }

  @Override
  public void mergeAtBottom(IncTaskBag that) {
    SyntheticBag taskBag = (SyntheticBag) that;
    tasks.pushArrayFirst(taskBag.tasks);
  }

  @Override
  public void mergeAtTop(IncTaskBag that) {
    SyntheticBag taskBag = (SyntheticBag) that;
    tasks.pushArrayLast(taskBag.tasks);
  }

  @Override
  public IncTaskBag removeFromBottom(long n) {
    SyntheticBag taskBag = new SyntheticBag((int) n);
    Object[] taskObjects = tasks.getFromFirst((int) n);
    System.arraycopy(taskObjects, 0, taskBag.tasks, 0, taskObjects.length);
    return taskBag;
  }

  @Override
  public IncTaskBag removeFromTop(long n) {
    SyntheticBag taskBag = new SyntheticBag((int) n);
    Object[] taskObjects = tasks.getFromLast((int) n);
    System.arraycopy(taskObjects, 0, taskBag.tasks, 0, taskObjects.length);
    return taskBag;
  }

  @Override
  public IncTaskBag getTopElement() {
    SyntheticBag taskBag = new SyntheticBag(1);
    taskBag.tasks[0] = tasks.peekLast();
    return taskBag;
  }

  @Override
  public IncTaskBag getFromBottom(long n, long offset) {
    SyntheticBag taskBag = new SyntheticBag((int) n);
    Object[] taskObjects = tasks.peekFromFirst((int) n, (int) offset);
    System.arraycopy(taskObjects, 0, taskBag.tasks, 0, taskObjects.length);
    return taskBag;
  }

  public static class SyntheticResult extends IncFTGLBResult<Long> implements Serializable {

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
