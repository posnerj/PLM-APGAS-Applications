package GLBCoop.examples.SyntheticBenchmark;

import GLBCoop.GLBResult;
import GLBCoop.TaskBag;
import GLBCoop.TaskQueue;
import java.io.Serializable;

public class Queue extends Synthetic implements TaskQueue<Queue, Long> {

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
  public GLBResult<Long> getResult() {
    return new SyntheticResult(this.result);
  }

  @Override
  public void printLog() {}

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
  public void mergeResult(TaskQueue<Queue, Long> that) {
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

  public static class SyntheticResult extends GLBResult<Long> implements Serializable {

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
