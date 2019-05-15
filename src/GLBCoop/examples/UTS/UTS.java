/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoop.examples.UTS;

import java.io.Serializable;
import utils.SHA1Rand;

public class UTS implements Serializable {

  private static final long serialVersionUID = 1L;
  public long count;
  protected SHA1Rand[] hash;
  protected int[] lower;
  protected int[] upper;
  protected int size;
  private double den;

  public UTS(int factor) {
    this.hash = new SHA1Rand[4096];
    this.lower = new int[4096];
    this.upper = new int[4096];
    this.size = 0;
    this.den = Math.log(factor / (1.0 + factor));
    this.count = 0;
  }

  public static String subString(String str, int start, int end) {
    return (str.substring(start, Math.min(end, str.length())));
  }

  public static void print(long time, long count) {
    System.out.println(
        "Performance: "
            + count
            + "/"
            + subString("" + time / 1e9, 0, 6)
            + " = "
            + subString("" + (count / (time / 1e3)), 0, 6)
            + "M nodes/timestamps");
  }

  public static void main(String[] args) {
    int d = 13;
    int b = 4;
    if (args.length > 1) {
      b = Integer.parseInt(args[0]);
      d = Integer.parseInt(args[1]);
    }
    UTS queue = new UTS(b); // branching factor
    System.out.println("Starting GLBCoop.examples.UTS.UTS..., b=" + b + ", d=" + d);
    long time = System.nanoTime();
    queue.init(19, d); // seed, depth
    while (queue.size > 0) {
      queue.expand();
      ++queue.count;
    }
    time = System.nanoTime() - time;
    System.out.println("Finished.");
    print(time, queue.count);
  }

  public final void init(int seed, int height) {
    this.push(new SHA1Rand(seed, height));
    ++count;
  }

  public void push(SHA1Rand sha1Rand) {
    int u = (int) Math.floor(Math.log(1.0 - sha1Rand.getRand() / 2147483648.0) / this.den);
    if (u > 0) {
      if (this.size >= this.hash.length) {
        this.grow();
      }
      this.hash[this.size] = sha1Rand;
      this.lower[this.size] = 0;
      this.upper[this.size++] = u;
    }
  }

  public void expand() {
    int top = this.size - 1;
    SHA1Rand h = this.hash[top];
    int d = h.getDepth();
    int l = this.lower[top];
    int u = this.upper[top] - 1;

    if (d > 1) {
      if (u == l) {
        --this.size;
      } else {
        this.upper[top] = u;
      }
      final SHA1Rand sha1Rand = new SHA1Rand(h, u, d - 1);
      this.push(sha1Rand);
    } else {
      --this.size;
      this.count += u - l;
    }
  }

  public void grow() {
    int capacity = this.size * 2;
    SHA1Rand[] h = new SHA1Rand[capacity];
    System.arraycopy(this.hash, 0, h, 0, this.size);
    this.hash = h;
    int[] l = new int[capacity];
    System.arraycopy(this.lower, 0, l, 0, this.size);
    this.lower = l;
    int[] u = new int[capacity];
    System.arraycopy(this.upper, 0, u, 0, this.size);
    this.upper = u;
  }
}
