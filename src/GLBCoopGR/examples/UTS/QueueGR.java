package GLBCoopGR.examples.UTS;

import static apgas.Constructs.here;

import GLBCoopGR.GLBResultGR;
import GLBCoopGR.TaskBagGR;
import GLBCoopGR.TaskQueueGR;
import java.io.Serializable;

public final class QueueGR extends UTS implements TaskQueueGR<QueueGR, Long>, Serializable {

  private static final long serialVersionUID = 1L;

  UTSResultGR result = null;

  public QueueGR(int factor, int numPlaces) {
    super(factor);
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
  public TaskBagGR split() {
    int s = 0;
    for (int i = 0; i < size; ++i) {
      if ((this.upper[i] - this.lower[i]) >= 2) {
        ++s;
      }
    }

    if (s == 0) {
      return null;
    }

    BagGR bag = new BagGR(s);
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

  public void merge(BagGR bag) {
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
  public void merge(TaskBagGR taskBagGR) {
    this.merge((BagGR) taskBagGR);
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public GLBResultGR<Long> getResult() {
    UTSResultGR result = new UTSResultGR();
    return result;
  }

  public void setResult(GLBResultGR<Long> r) {
    count = r.getResult()[0];
  }

  public void setResult(QueueGR q) {
    count = q.count;
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(TaskQueueGR<QueueGR, Long> other) {
    if (other == null) {
      System.err.println(here() + "(in mergeResult): other is null");
    }
    count += other.count();
  }

  @Override
  public int size() {
    return this.size;
  }

  public void mergeResult(QueueGR q) {
    count += q.count;
  }

  public class UTSResultGR extends GLBResultGR<Long> {

    private static final long serialVersionUID = 1L;

    Long[] result;

    public UTSResultGR() {
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
