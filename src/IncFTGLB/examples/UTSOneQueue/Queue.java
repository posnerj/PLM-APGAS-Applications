package IncFTGLB.examples.UTSOneQueue;

import static apgas.Constructs.here;

import GLBCoop.examples.UTSOneQueue.TreeNode;
import GLBCoop.examples.UTSOneQueue.UTS;
import IncFTGLB.IncFTGLBResult;
import IncFTGLB.IncFTTaskQueue;
import IncFTGLB.IncTaskBag;
import java.io.Serializable;

public final class Queue extends UTS implements IncFTTaskQueue<Queue, Long>, Serializable {

  private static final long serialVersionUID = 1L;

  public Queue(int factor) {
    super(factor);
  }

  public Queue(int factor, int size) {
    super(factor, size);
  }

  public Queue(double den, int size) {
    super(den, size);
  }

  @Override
  public void process() {
    this.expand();
    ++count;
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
  public IncFTGLBResult<Long> getResult() {
    UTSResult result = new UTSResult();
    return result;
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  @Override
  public void setResult(IncFTGLBResult<Long> r) {
    count = r.getResult()[0];
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(IncFTTaskQueue<Queue, Long> that) {
    if (that == null) {
      System.err.println(here() + "(in mergeResult): that is null");
    }
    count += that.count();
  }

  @Override
  public void mergeAtTop(IncTaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    this.pushArrayLast(bag.hash);
  }

  @Override
  public void mergeAtBottom(IncTaskBag taskBag) {
    Bag bag = (Bag) taskBag;
    this.pushArrayFirst(bag.hash);
  }

  @Override
  public IncTaskBag removeFromTop(long n) {
    Bag bag = new Bag();
    bag.hash = this.getFromLast((int) n);
    return bag;
  }

  @Override
  public IncTaskBag removeFromBottom(long n) {
    Bag bag = new Bag();
    bag.hash = this.getFromFirst((int) n);
    return bag;
  }

  @Override
  public IncTaskBag getFromBottom(long n, long offset) {
    Bag bag = new Bag();
    bag.hash = this.peekFromFirst((int) n, (int) offset);
    return bag;
  }

  @Override
  public IncTaskBag getTopElement() {
    Bag bag = new Bag();
    if (size() > 0) {
      bag.hash = new TreeNode[] {this.peekLast()};
    } else {
      bag.hash = new TreeNode[0];
    }
    return bag;
  }

  @Override
  public IncTaskBag split() {
    if (size() <= 1) {
      return null;
    }

    int otherHalf = (int) Math.max(this.size() * 0.1, 1);

    if (0 == otherHalf) {
      return null;
    }

    Bag bag = new Bag();
    bag.hash = super.getFromFirst(otherHalf);
    return bag;
  }

  public class UTSResult extends IncFTGLBResult<Long> implements Serializable {

    private static final long serialVersionUID = 1L;

    long result;

    public UTSResult() {
      this.result = count;
    }

    public Long[] getResult() {
      return new Long[] {result};
    }

    @Override
    public void display(Long[] r) {
      System.out.println("Myresult: " + r[0]);
    }
  }
}
