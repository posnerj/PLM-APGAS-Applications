package FTGLB.examples.Pi;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import FTGLB.FTGLBResult;
import FTGLB.FTTaskQueue;
import FTGLB.TaskBag;
import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;

public class Queue implements FTTaskQueue<Queue, Double>, Serializable {

  int N;

  Deque<Integer> list;

  double result;

  double deltaX;

  // Fuer Paper raus
  int count;

  // Dynamische Start Verteilung
  public Queue(int n, boolean dynDist) {
    this.N = n;
    this.result = 0;
    this.count = 0;
    this.deltaX = 1.0 / this.N;
    this.list = new LinkedList<>();
    if (dynDist == false) {
      int myStep = this.N / places().size();
      int start = here().id * myStep;
      int end = Math.min(start + myStep, this.N);

      for (int i = start; i < end; i++) {
        this.list.add(i);
      }
    }
  }

  // Dynamische Start Verteilung
  public void init() {
    for (int i = 0; i < this.N; i++) {
      list.add(i);
    }
  }

  @Override
  public boolean process(int n) {
    double tmp = 0;
    for (int i = 0; i < n; i++) {

      double x = list.pop() * deltaX;
      tmp += 4.0 / (1 + x * x);

      this.count++;

      if (size() <= 0) {
        break;
      }
    }

    tmp *= deltaX;
    result += tmp;
    return (size() > 0);
  }

  @Override
  public TaskBag split() {
    int size = this.list.size() / 2;
    if (size <= 0) {
      return null;
    }

    Bag bag = new Bag();
    for (int i = 0; i < size; i++) {
      bag.list.add(this.list.poll());
    }
    return bag;
  }

  @Override
  public void merge(TaskBag taskBag) {
    this.list.addAll(((Bag) taskBag).list);
  }

  // Fuer Paper raus
  @Override
  public long count() {
    return this.count;
  }

  @Override
  public FTGLBResult<Double> getResult() {
    return new PiResult();
  }

  // Fuer Paper raus
  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(FTTaskQueue<Queue, Double> that) {
    this.result += that.getResult().getResult()[0];
  }

  @Override
  public int size() {
    return this.list.size();
  }

  @Override
  public void clearTasks() {
    this.list = new LinkedList<>();
  }

  @Override
  public TaskBag getAllTasks() {
    Bag bag = new Bag();
    bag.list.addAll(this.list);
    return bag;
  }

  public class PiResult extends FTGLBResult<Double> {

    @Override
    public Double[] getResult() {
      return new Double[] {result};
    }

    // Fuer Paper raus
    @Override
    public void display(Double[] r) {
      System.out.println("Myresult: " + r[0]);
    }
  }
}
