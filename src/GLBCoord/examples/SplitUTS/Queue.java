/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoord.examples.SplitUTS;

import static apgas.Constructs.here;

import GLBCoop.GLBResult;
import GLBCoop.TaskBag;
import GLBCoop.examples.UTSOneQueue.Bag;
import GLBCoord.TaskQueueCoord;
import utils.ConsolePrinter;

public final class Queue extends UTS implements TaskQueueCoord<Queue, Long> {

  private static final long serialVersionUID = 1L;

  public static double splitFactor = 0.5;

  UTSResult result = null;
  ConsolePrinter consolePrinter;

  public Queue(int factor) {
    super(factor);
    this.consolePrinter = ConsolePrinter.getInstance();
  }

  public Queue(int factor, double split) {
    super(factor, split);
    this.consolePrinter = ConsolePrinter.getInstance();
  }

  public Queue(int factor, double split, double steal) {
    super(factor, split);
    Queue.splitFactor = steal;
    this.consolePrinter = ConsolePrinter.getInstance();
  }

  public Queue(int factor, int size) {
    super(factor, size);
    this.consolePrinter = ConsolePrinter.getInstance();
  }

  public Queue(double den, int size) {
    super(den, size);
    this.consolePrinter = ConsolePrinter.getInstance();
  }

  @Override
  public boolean process(int n) {
    int i = 0;
    for (; ((i < n) && (privateSize() > 0)); ++i) { // this.size()
      expand();
    }
    count += i;
    return (privateSize() > 0);
  }

  @Override
  public synchronized TaskBag split() {
    consolePrinter.println(
        here()
            + "split() starts at "
            + here()
            + " , size: "
            + size()
            + ", publicSize: "
            + publicSize());

    int otherHalf = (int) ((publicSize() + 1) * splitFactor);
    if (otherHalf > publicSize()) {
      otherHalf = publicSize();
    }

    if (0 == otherHalf) {
      consolePrinter.println(here() + "split() ends at " + here() + " with null");
      return null;
    }
    Bag bag = new Bag(otherHalf);
    bag.hash = super.popPublic(otherHalf);
    consolePrinter.println(
        here()
            + "split() ends at "
            + here()
            + " , size: "
            + size()
            + ", publicSize: "
            + publicSize());
    return bag;
  }

  public void mergePublic(Bag bag) {
    consolePrinter.println(here() + "mergePublic() starts at " + here() + " : " + size());
    pushPublic(bag.hash);
    consolePrinter.println(here() + "mergePublic() ends at " + here() + " : " + size());
  }

  public void mergePublic(TaskBag taskBag) {
    mergePublic((Bag) taskBag);
  }

  @Override
  public long count() {
    return this.count;
  }

  @Override
  public GLBResult<Long> getResult() {
    return new UTSResult();
  }

  public void setResult(GLBResult<Long> r) {
    count = r.getResult()[0];
  }

  public void setResult(Queue q) {
    count = q.count;
  }

  @Override
  public void printLog() {}

  @Override
  public void mergeResult(TaskQueueCoord<Queue, Long> that) {
    consolePrinter.println("In mergeResult: " + (that != null));
    count += that.count();
  }

  @Override
  public int getCountRelease() {
    return this.countRelease;
  }

  @Override
  public int getCountRequire() {
    return this.countReacquire;
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

  public class UTSResult extends GLBResult<Long> {

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
      consolePrinter.println("Myresult: " + r[0]);
    }
  }
}
