package IncFTGLB.examples.PrimeOneQueue;

import static apgas.Constructs.here;

import IncFTGLB.IncFTGLBResult;
import IncFTGLB.IncFTTaskQueue;
import IncFTGLB.IncTaskBag;
import java.io.Serializable;

public class Queue extends Prime implements IncFTTaskQueue<Queue, Integer>, Serializable {

  private static final long serialVersionUID = 1L;

  public Queue(int min, int max, int numPlaces) {
    super();
    final int totalTasks = max - min + 1;
    final int tasksPerPlace = (totalTasks + numPlaces - 1) / numPlaces;
    min = here().id * tasksPerPlace;
    max = Math.min(min + tasksPerPlace - 1, max);
    init(min, max);
  }

  @Override
  public void process() {
    next();
  }

  @Override
  public IncTaskBag split() {
    int otherHalf = size() / 2;

    if (0 == otherHalf) {
      return null;
    }

    Bag bag = new Bag();

    for (int i = 0; i < otherHalf; ++i) {
      bag.deque.addLast(deque.pollFirst());
    }

    return bag;
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public void setCount(long count) {
    this.count = (int) count;
  }

  @Override
  public IncFTGLBResult<Integer> getResult() {
    PrimeResult result = new PrimeResult();
    return result;
  }

  @Override
  public void setResult(IncFTGLBResult<Integer> result) {
    this.count = result.getResult()[0];
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(IncFTTaskQueue<Queue, Integer> other) {
    count += other.count();
  }

  @Override
  public int size() {
    return deque.size();
  }

  @Override
  public void mergeAtBottom(IncTaskBag that) {
    Bag bag = (Bag) that;

    Integer[] array = bag.deque.toArray(new Integer[bag.size()]);
    for (int i = bag.size() - 1; i >= 0; --i) {
      deque.addFirst(array[i]);
    }
  }

  @Override
  public void mergeAtTop(IncTaskBag that) {
    Bag bag = (Bag) that;

    Integer[] array = bag.deque.toArray(new Integer[bag.size()]);
    for (int i = 0; i < bag.size(); ++i) {
      deque.addLast(array[i]);
    }
  }

  @Override
  public IncTaskBag removeFromBottom(long n) {
    Bag bag = new Bag();
    for (int i = 0; i < n; ++i) {
      bag.deque.addLast(deque.pollFirst());
    }
    return bag;
  }

  @Override
  public IncTaskBag removeFromTop(long n) {
    Bag bag = new Bag();
    for (int i = 0; i < n; ++i) {
      bag.deque.addFirst(deque.pollLast());
    }
    return bag;
  }

  @Override
  public IncTaskBag getTopElement() {
    Bag bag = new Bag();
    if (size() > 0) {
      bag.deque.addLast(deque.peekLast());
    }
    return bag;
  }

  @Override
  public IncTaskBag getFromBottom(long n, long offset) {
    Bag bag = new Bag();

    Integer[] array = deque.toArray(new Integer[deque.size()]);
    for (int i = (int) offset; i < n + offset; ++i) {
      bag.deque.addLast(array[i]);
    }

    return bag;
  }

  public class PrimeResult extends IncFTGLBResult<Integer> implements Serializable {

    private static final long serialVersionUID = 1L;

    private int result;

    public PrimeResult() {
      result = count;
    }

    @Override
    public Integer[] getResult() {
      return new Integer[] {result};
    }

    @Override
    public void display(Integer[] param) {
      System.out.println("Myresult: " + param[0]);
    }
  }
}
