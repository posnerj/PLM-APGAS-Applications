/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package GLBCoord.examples.SplitUTS;

import GLBCoop.examples.UTSOneQueue.TreeNode;
import utils.SHA1Rand;
import utils.SplitQueue;

public class UTS extends SplitQueue<TreeNode> {

  private static final long serialVersionUID = 1L;
  public final double den;
  public long count;

  public UTS(int factor) {
    this(factor, 8);
  }

  public UTS(int factor, double split) {
    this(factor, 8, split);
  }

  public UTS(int factor, int size) {
    super(size, TreeNode.class);
    den = Math.log(factor / (1.0 + factor));
    this.count = 0;
  }

  public UTS(int factor, int size, double split) {
    super(size, TreeNode.class, split);
    den = Math.log(factor / (1.0 + factor));
    this.count = 0;
  }

  public UTS(double den, int size) {
    super(size, TreeNode.class);
    this.den = den;
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
    UTS queue = new UTS(4); // branching factor
    System.out.println("Starting...");
    long time = System.nanoTime();
    queue.init(19, 13); // seed, depth
    while (queue.privateSize() > 0) {
      queue.expand();
      ++queue.count;
    }
    time = System.nanoTime() - time;
    System.out.println("Finished.");
    print(time, queue.count);
  }

  public void init(int seed, int height) {
    SHA1Rand sha1Rand = new SHA1Rand(seed, height);
    push(sha1Rand);
    ++this.count;
  }

  public void push(SHA1Rand sha1Rand) {
    int u = (int) Math.floor(Math.log(1 - sha1Rand.getRand() / 2147483648.0) / this.den);

    if (sha1Rand.getDepth() > 1) {
      TreeNode[] newNodes = new TreeNode[u];
      for (int i = 0; i < u; i++) {
        newNodes[i] = new TreeNode(sha1Rand, i);
      }
      this.pushPrivate(newNodes);
    } else {
      this.count += u;
    }
  }

  public void expand() {
    TreeNode treeNode = this.popPrivate();
    int depth = treeNode.getParent().getDepth();
    if (depth > 1) {
      SHA1Rand sha1Rand = new SHA1Rand(treeNode.getParent(), treeNode.getId(), depth - 1);
      this.push(sha1Rand);
    }
  }
}
