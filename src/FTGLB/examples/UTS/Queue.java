package FTGLB.examples.UTS;

import static apgas.Constructs.here;

import FTGLB.FTGLBResult;
import FTGLB.FTTaskQueue;
import GLBCoop.TaskBag;
import GLBCoop.examples.UTS.UTS;
import java.io.Serializable;
import java.util.Arrays;
import utils.SHA1Rand;

public final class Queue extends UTS implements FTTaskQueue<Queue, Long>, Serializable {

  private static final long serialVersionUID = 1L;

  long[] receivedLids;
  long myLid;
  boolean done;

  public Queue(int factor, int numPlaces) {
    super(factor);
    this.receivedLids = new long[numPlaces];
    Arrays.setAll(this.receivedLids, i -> Long.MIN_VALUE); // i is the array index
    this.myLid = Long.MIN_VALUE;
    this.done = false;
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
  public TaskBag split() {
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
  public void merge(TaskBag taskBag) {
    this.merge((Bag) taskBag);
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

  public void setResult(FTGLBResult<Long> r) {
    count = r.getResult()[0];
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(FTTaskQueue<Queue, Long> other) {
    if (other == null) {
      System.err.println(here() + "(in mergeResult): other is null");
    }
    count += other.count();
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public void clearTasks() {
    super.hash = new SHA1Rand[4096];
    super.lower = new int[4096];
    super.upper = new int[4096];
    super.size = 0;
  }

  @Override
  public TaskBag getAllTasks() {
    final int s = this.size();
    Bag bag = new Bag(s);
    System.arraycopy(this.hash, 0, bag.hash, 0, s);
    System.arraycopy(this.lower, 0, bag.lower, 0, s);
    System.arraycopy(this.upper, 0, bag.upper, 0, s);
    return bag;
  }

  public void mergeResult(Queue q) {
    count += q.count;
  }

  public class UTSResult extends FTGLBResult<Long> {

    private static final long serialVersionUID = 1L;

    Long[] result;

    public UTSResult() {
      this.result = new Long[1];
    }

    public Long[] getResult() {
      result[0] = count;
      return result;
    }

    @Override
    public void display(Long[] r) {
      System.out.println("Myresult: " + r[0]);
    }
  }
}
