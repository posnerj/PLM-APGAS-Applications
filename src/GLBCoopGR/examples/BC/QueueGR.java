package GLBCoopGR.examples.BC;

import static apgas.Constructs.here;
import static apgas.Constructs.places;

import GLBCoopGR.GLBResultGR;
import GLBCoopGR.TaskBagGR;
import GLBCoopGR.TaskQueueGR;
import utils.Rmat;

public class QueueGR extends BC implements TaskQueueGR<QueueGR, Double> {

  public int[] lower;
  public int[] upper;
  public int state = 0;
  public int s;
  protected int size;

  public QueueGR(Rmat rmat, int permute, int numPlaces) {
    super(rmat, permute);
    this.lower = new int[4096];
    this.upper = new int[4096];
    final int h = here().id;
    final int max = places().size();
    this.lower[0] = (int) ((long) this.N * h / max);
    this.upper[0] = (int) ((long) this.N * (h + 1) / max);
    this.size = 1;
  }

  public void grow() {
    int capacity = this.size * 2;
    int[] l = new int[capacity];
    System.arraycopy(this.lower, 0, l, 0, this.size);
    this.lower = l;
    int[] u = new int[capacity];
    System.arraycopy(this.upper, 0, u, 0, this.size);
    this.upper = u;
  }

  @Override
  public boolean process(int n) {
    int i = 0;

    switch (state) {
      case 0:
        int top = this.size - 1;
        final int l = this.lower[top];
        final int u = this.upper[top] - 1;
        if (u == l) {
          this.size--;
        } else {
          this.upper[top] = u;
        }
        refTime = System.nanoTime();
        try {
          s = this.verticesToWorkOn[u];
        } catch (Exception e) {
          System.out.println(
              here()
                  + " exception, lower.length "
                  + this.lower.length
                  + ", upper.length "
                  + this.upper.length
                  + ", verticesToWorkOn.length "
                  + this.verticesToWorkOn.length
                  + ", n "
                  + n
                  + ", s "
                  + s);
          e.printStackTrace(System.out);
        }
        this.state = 1;

      case 1:
        this.bfsShortestPath1(s);
        this.state = 2;

      case 2:
        while (!regularQueue.isEmpty()) {
          if (i++ > n) {
            return true;
          }
          this.bfsShortestPath2();
        }
        this.state = 3;

      case 3:
        this.bfsShortestPath3();
        this.state = 4;

      case 4:
        while (!regularQueue.isEmpty()) {
          if (i++ > n) {
            return true;
          }
          this.bfsShortestPath4(s);
        }
        this.accTime += ((System.nanoTime() - refTime) / 1e9);
        this.state = 0;
    }
    return (0 < this.size);
  }

  @Override
  public TaskBagGR split() {
    int s = 0;
    for (int i = 0; i < this.size; ++i) {
      if (2 <= (this.upper[i] - this.lower[i])) {
        ++s;
      }
    }

    if (s == 0) {
      return null;
    }

    BagGR bag = new BagGR(s);
    s = 0;
    for (int i = 0; i < this.size; i++) {
      int p = this.upper[i] - this.lower[i];
      if (2 <= p) {
        // bag.upper(s) = upper(i);
        // upper(i) -= (p / 2n);
        // bag.lower(s++) = upper(i);
        bag.lower[s] = this.lower[i];
        bag.upper[s] = this.upper[i] - ((p + 1) / 2);
        this.lower[i] = bag.upper[s++];
      }
    }
    return (bag);
  }

  public void merge(BagGR bag) {
    int bagSize = bag.size();
    int thisSize = this.size;
    while (this.upper.length < bagSize + thisSize) {
      grow();
    }
    System.arraycopy(this.lower, 0, this.lower, bagSize, thisSize);
    System.arraycopy(this.upper, 0, this.upper, bagSize, thisSize);

    System.arraycopy(bag.lower, 0, this.lower, 0, bagSize);
    System.arraycopy(bag.upper, 0, this.upper, 0, bagSize);
    this.size += bagSize;
  }

  @Override
  public void merge(TaskBagGR taskBagGR) {
    this.merge((BagGR) taskBagGR);
  }

  public void merge(TaskQueueGR<QueueGR, Double> other) {
    this.merge((QueueGR) other);
  }

  public void merge(QueueGR other) {
    int thisSize = this.size();
    int otherSize = (null == other ? 0 : other.size());
    if (0 == otherSize) {
      return;
    }

    while (thisSize + otherSize > this.lower.length) {
      this.grow();
    }

    System.arraycopy(other.lower, 0, this.lower, thisSize, otherSize);
    System.arraycopy(other.upper, 0, this.upper, thisSize, otherSize);
    this.size += otherSize;
  }

  @Override
  public void mergeResult(TaskQueueGR<QueueGR, Double> other) {
    this.mergeResult((QueueGR) other);
  }

  public void mergeResult(QueueGR other) {
    for (int i = 0; i < other.realBetweennessMap.length; ++i) {
      this.realBetweennessMap[i] += other.realBetweennessMap[i];
    }
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public void printLog() {
    System.out.println(
        "[" + here().id + "]" + " Time = " + this.accTime + " Count = " + this.count);
  }

  @Override
  public GLBResultGR<Double> getResult() {
    BCGResultGR result = new BCGResultGR();
    return result;
  }

  public class BCGResultGR extends GLBResultGR<Double> {

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
