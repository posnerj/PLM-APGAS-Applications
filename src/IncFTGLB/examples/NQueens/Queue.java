/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package IncFTGLB.examples.NQueens;

import static apgas.Constructs.here;

import GLBCoop.examples.NQueens.NQueens;
import IncFTGLB.IncFTGLBResult;
import IncFTGLB.IncFTTaskQueue;
import IncFTGLB.IncTaskBag;
import java.io.Serializable;
import java.util.Arrays;

/** Created by jposner on 05.07.17. */
public final class Queue extends NQueens implements IncFTTaskQueue<Queue, Long>, Serializable {

  public Queue() {
    super();
  }

  public Queue(int numPlaces) {
    super();
  }

  public Queue(int size, int threshold, int numPlaces) {
    super(size, threshold);
  }

  public static void main(String[] args) {
    int[] one = {1};
    int[] two = {2};
    int[] three = {3};
    int[] four = {4};
    int[] five = {5};

    Queue queue = new Queue(1);
    queue.init();
    queue.push(one, 1);
    queue.push(two, 2);
    queue.push(three, 3);
    queue.push(four, 4);
    queue.push(five, 5);
    System.out.println(queue);

    IncTaskBag split = queue.split();
    Bag bag = (Bag) split;
    System.out.println(queue);
    System.out.println(bag);

    queue.mergeAtBottom(split);
    System.out.println(queue);

    Queue emptyQueue = new Queue(1);
    emptyQueue.mergeAtBottom(bag);
    emptyQueue.mergeAtBottom(bag);
    emptyQueue.mergeAtBottom(bag);
    System.out.println(emptyQueue);
  }

  @Override
  public void process() {
    this.nqueensKernelPar();
  }

  @Override
  public IncTaskBag split() {
    if (2 > this.size()) {
      return null;
    }

    int otherHalf = this.size * 1 / 6;
    if (otherHalf == 0) {
      otherHalf = 1;
    }

    return removeFromBottom(otherHalf);
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public IncFTGLBResult<Long> getResult() {
    NQueensResult result = new NQueensResult();
    return result;
  }

  @Override
  public void setResult(IncFTGLBResult<Long> result) {
    this.count = result.getResult()[0];
  }

  @Override
  public void printLog() {
    return;
  }

  @Override
  public void mergeResult(IncFTTaskQueue<Queue, Long> that) {
    if (that == null) {
      System.err.println(here() + "(in mergeResult): other is null");
    }
    count += that.count();
  }

  @Override
  public int size() {
    return this.getSize();
  }

  public void init() {
    push(new int[0], 0);
  }

  @Override
  public String toString() {
    return "Queue{"
        + "a="
        + aToString()
        + ", depth="
        + Arrays.toString(depth)
        + ", size="
        + size
        + '}';
  }

  public String aToString() {
    String result = new String();
    for (int i = 0; i < this.depth.length; i++) {
      if (a[i] != null) {
        if (a[i].length > 0) {
          result += a[i][0] + " ";
        } else {
          result += "null ";
        }
      } else {
        result += "null ";
      }
    }
    return result;
  }

  @Override
  public void setCount(long count) {
    this.count = count;
  }

  @Override
  public void mergeAtBottom(IncTaskBag that) {
    final int thisSize = this.size();
    final int thatSize = that.size();
    final int newSize = thisSize + thatSize;

    while (newSize > this.depth.length) {
      this.grow();
    }

    final Bag bag = (Bag) that;

    System.arraycopy(this.a, 0, this.a, thatSize, thisSize);
    System.arraycopy(this.depth, 0, this.depth, thatSize, thisSize);

    System.arraycopy(bag.a, 0, this.a, 0, thatSize);
    System.arraycopy(bag.depth, 0, this.depth, 0, thatSize);

    this.size += thatSize;
  }

  @Override
  public void mergeAtTop(IncTaskBag that) {
    final Bag bag = (Bag) that;
    final int thisSize = this.size();
    final int thatSize = that.size();
    final int newSize = thisSize + thatSize;

    while (newSize > this.depth.length) {
      this.grow();
    }

    System.arraycopy(bag.a, 0, this.a, thisSize, thatSize);
    System.arraycopy(bag.depth, 0, this.depth, thisSize, thatSize);

    this.size += thatSize;
  }

  @Override
  public IncTaskBag removeFromBottom(long n) {
    final int newSize = this.size() - (int) n;

    Bag bag = (Bag) getFromBottom(n, 0);
    System.arraycopy(this.a, (int) n, this.a, 0, newSize);
    System.arraycopy(this.depth, (int) n, this.depth, 0, newSize);

    this.size -= n;

    return bag;
  }

  @Override
  public IncTaskBag removeFromTop(long n) {
    final Bag bag = (Bag) getFromBottom(n, this.size - n);
    this.size -= n;
    return bag;
  }

  @Override
  public IncTaskBag getTopElement() {
    if (this.size() == 0) {
      return new Bag(0);
    }

    final Bag bag = new Bag(1);

    int top = this.size - 1;
    bag.a[0] = a[top];
    bag.depth[0] = depth[top];

    return bag;
  }

  @Override
  public IncTaskBag getFromBottom(long n, long offset) {
    final Bag bag = new Bag((int) n);

    System.arraycopy(this.a, (int) offset, bag.a, 0, (int) n);
    System.arraycopy(this.depth, (int) offset, bag.depth, 0, (int) n);

    return bag;
  }

  public class NQueensResult extends IncFTGLBResult<Long> {

    Long[] result;

    public NQueensResult() {
      this.result = new Long[1];
    }

    @Override
    public Long[] getResult() {
      this.result[0] = count();
      return this.result;
    }

    @Override
    public void display(Long[] param) {}
  }
}
