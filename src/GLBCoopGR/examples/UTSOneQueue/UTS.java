package GLBCoopGR.examples.UTSOneQueue;

import java.io.Serializable;
import utils.SHA1Rand;

public class UTS extends TreeNodeDeque implements Serializable {

  private static final long serialVersionUID = 1L;
  public final double den;
  public long count;

  public UTS(int factor) {
    this(factor, 4096);
  }

  public UTS(int factor, int size) {
    super(size);
    den = Math.log(factor / (1.0 + factor));
    this.count = 0;
  }

  public UTS(double den, int size) {
    super(size);
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
    System.out.println("Starting FTGLB.UTSOneQueue.UTS...");
    long time = System.nanoTime();
    queue.init(19, 13); // seed, depth
    while (queue.size() > 0) {
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
      this.pushArrayLast(newNodes);
    } else {
      this.count += u;
    }
  }

  public void expand() {
    TreeNode treeNode = this.pollLast();
    int depth = treeNode.getParent().getDepth();
    if (depth > 1) {
      SHA1Rand sha1Rand = new SHA1Rand(treeNode.getParent(), treeNode.getId(), depth - 1);
      this.push(sha1Rand);
    }
  }
}
