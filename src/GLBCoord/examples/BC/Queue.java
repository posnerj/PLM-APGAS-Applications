package GLBCoord.examples.BC;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import GLBCoop.GLBResult;
import GLBCoop.TaskBag;
import GLBCoord.TaskQueueCoord;
import utils.ConsolePrinter;
import utils.Rmat;

public class Queue extends BC implements TaskQueueCoord<Queue, Double> {

  private static double splitFactor = 0.5;
  ConsolePrinter consolePrinter;
  private int state;
  private int s;

  public Queue(Rmat rmat, int permute) {
    super(rmat, permute);

    consolePrinter = ConsolePrinter.getInstance();
    this.state = 0;
    this.s = 0;
    int max = places().size();
    int h = here().id;
    int lower = (int) ((long) (N) * h / max);
    int upper = (int) ((long) (N) * (h + 1) / max);
    int size = upper - lower;

    for (int i = 0; i < size; i++) {
      pushPrivate(i + lower);
    }
  }

  public Queue(Rmat rmat, int permute, double split, double steal) {
    super(rmat, permute, split);

    consolePrinter = ConsolePrinter.getInstance();
    this.state = 0;
    this.s = 0;
    int max = places().size();
    int h = here().id;
    int lower = (int) ((long) (N) * h / max);
    int upper = (int) ((long) (N) * (h + 1) / max);
    int size = upper - lower;

    for (int i = 0; i < size; i++) {
      pushPrivate(i + lower);
    }
    splitFactor = steal;
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
  public GLBResult<Double> getResult() {
    return new BCGResult();
  }

  public void merge(TaskBag taskBag) {
    merge((Bag) taskBag);
  }

  public void merge(Bag bag) {
    consolePrinter.println("merge() start at " + here() + " : " + size());
    pushPrivate(bag.data);
    consolePrinter.println("merge() end at " + here() + " : " + size());
  }

  public void mergePublic(TaskBag taskBag) {
    merge((Bag) taskBag);
  }

  public void mergePrivate(TaskBag taskBag) {
    merge((Bag) taskBag);
  }

  @Override
  public void mergeResult(TaskQueueCoord<Queue, Double> that) {
    mergeResult((Queue) that);
  }

  public void mergeResult(Queue that) {
    assert (this.betweennessMap.length == that.betweennessMap.length)
        : here() + ": cannot set result, rails do not match in length!";

    for (int i = 0; i < this.betweennessMap.length; i++) {
      this.betweennessMap[i] += that.betweennessMap[i];
    }
  }

  @Override
  public void printLog() {
    consolePrinter.println("[" + here() + "]" + " Count = " + count);
  }

  @Override
  public boolean process(int n) {
    int i = 0;
    switch (state) {
      case 0:
        int u = popPrivate();
        this.s = this.verticesToWorkOn[u];
        this.state = 1;
      case 1:
        bfsShortestPath1(s);
        this.state = 2;
      case 2:
        while (!regularQueue.isEmpty()) {
          if (i++ > n) {
            return (true);
          }
          bfsShortestPath2();
        }
        this.state = 3;
      case 3:
        bfsShortestPath3();
        this.state = 4;
      case 4:
        while (!regularQueue.isEmpty()) {
          if (i++ > n) {
            return (true);
          }
          bfsShortestPath4(s);
        }
        this.state = 0;
    }
    return (0 < size());
  }

  public int getHead() {
    return this.head;
  }

  public int getTail() {
    return this.tail;
  }

  public int getSplit() {
    return this.split;
  }

  @Override
  public synchronized TaskBag split() {
    consolePrinter.println(
        "split() start at " + here() + " , size: " + size() + ", publicSize: " + publicSize());

    int otherHalf = (int) ((publicSize() + 1) * splitFactor);

    if (otherHalf > publicSize()) {
      otherHalf = publicSize();
    }

    if (0 == otherHalf) {
      consolePrinter.println("split() ends at " + here() + " with null");
      return null;
    }

    Bag bag = new Bag(otherHalf);
    bag.data = super.popPublic(otherHalf);
    consolePrinter.println(
        "split() end at " + here() + " , size: " + size() + ", publicSize: " + publicSize());
    return bag;
  }

  @Override
  public int getCountRelease() {
    return this.countRelease;
  }

  @Override
  public int getCountRequire() {
    return this.countReacquire;
  }

  private class BCGResult extends GLBResult<Double> {

    @Override
    public Double[] getResult() {
      return betweennessMap;
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
