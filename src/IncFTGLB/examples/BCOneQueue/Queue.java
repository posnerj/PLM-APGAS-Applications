package IncFTGLB.examples.BCOneQueue;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import GLBCoop.examples.BC.BC;
import IncFTGLB.IncFTGLBResult;
import IncFTGLB.IncFTTaskQueue;
import IncFTGLB.IncTaskBag;
import java.io.Serializable;
import utils.MyIntegerDeque;
import utils.Rmat;

public class Queue extends BC implements IncFTTaskQueue<Queue, Double>, Serializable {

  private final MyIntegerDeque deque;

  private transient int s;
  private transient int state;

  public Queue(Rmat rmat, int permute, int numPlaces) {
    super(rmat, permute);
    this.s = 0;
    int max = places().size();
    int h = here().id;
    int lower = (int) ((long) (N) * h / max);
    int upper = (int) ((long) (N) * (h + 1) / max);
    int size = upper - lower;

    this.deque = new MyIntegerDeque(size);
    for (int i = 0; i < size; i++) {
      deque.offerLast(i + lower);
    }
  }

  /** substring helper function */
  public static String sub(String str, int start, int end) {
    return (str.substring(start, Math.min(end, str.length())));
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public void setCount(long count) {
    this.count = count;
  }

  @Override
  public IncFTGLBResult<Double> getResult() {
    BCGResult result = new BCGResult();
    return result;
  }

  @Override
  public void setResult(IncFTGLBResult<Double> result) {
    Double[] dResult = result.getResult();
    this.realBetweennessMap = new double[dResult.length];
    for (int i = 0; i < dResult.length; i++) {
      this.realBetweennessMap[i] = dResult[i].doubleValue();
    }
  }

  @Override
  public void mergeAtTop(IncTaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    if (bag.size() == 0) {
      return;
    }

    this.deque.pushArrayLast(bag.data);
  }

  @Override
  public void mergeAtBottom(IncTaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    if (bag.size() == 0) {
      return;
    }

    this.deque.pushArrayFirst(bag.data);
  }

  @Override
  public void mergeResult(IncFTTaskQueue<Queue, Double> that) {
    mergeResult((Queue) that);
  }

  public void mergeResult(Queue that) {
    for (int i = 0; i < this.realBetweennessMap.length; i++) {
      this.realBetweennessMap[i] += that.realBetweennessMap[i];
    }
  }

  @Override
  public void printLog() {
    System.out.println("[" + here() + "]" + " Count = " + count);
  }

  @Override
  public void process() {
    switch (state) {
      case 0:
        int u = deque.removeLast();
        s = this.verticesToWorkOn[u];
        this.state = 1;

      case 1:
        this.bfsShortestPath1(s);
        this.state = 2;

      case 2:
        while (!regularQueue.isEmpty()) {
          this.bfsShortestPath2();
        }
        this.state = 3;

      case 3:
        this.bfsShortestPath3();
        this.state = 4;

      case 4:
        while (!regularQueue.isEmpty()) {
          this.bfsShortestPath4(s);
        }
        this.state = 0;
    }
  }

  @Override
  public int size() {
    return this.deque.size();
  }

  @Override
  public IncTaskBag split() {
    int otherHalf = (int) (this.size() * 0.5);

    if (0 == otherHalf) {
      return null;
    }

    Bag bag = new Bag();
    bag.data = deque.getFromFirst(otherHalf);
    return bag;
  }

  @Override
  public IncTaskBag removeFromTop(long n) {
    Bag bag = new Bag();
    bag.data = deque.getFromLast((int) n);
    return bag;
  }

  @Override
  public IncTaskBag removeFromBottom(long n) {
    Bag bag = new Bag();
    bag.data = deque.getFromFirst((int) n);
    return bag;
  }

  @Override
  public IncTaskBag getFromBottom(long n, long offset) {
    Bag bag = new Bag();
    bag.data = deque.peekFromFirst((int) n, (int) offset);
    return bag;
  }

  @Override
  public IncTaskBag getTopElement() {
    Bag bag = new Bag();
    if (size() > 0) {
      bag.data = new int[] {deque.peekLast()};
    } else {
      bag.data = new int[0];
    }
    return bag;
  }

  private class BCGResult extends IncFTGLBResult<Double> {

    @Override
    public Double[] getResult() {
      Double[] result = new Double[realBetweennessMap.length];
      for (int i = 0; i < realBetweennessMap.length; i++) {
        result[i] = realBetweennessMap[i];
      }
      return result;
    }

    @Override
    public void display(Double[] r) {
      for (int i = 0; i < N; ++i) {
        if (0.0 != r[i]) {
          System.out.println("(" + i + ") -> " + sub("" + r[i], 0, 6));
        }
      }
    }
  }
}
