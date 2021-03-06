/*
 * This file is licensed to You under the Eclipse Public License (EPL);
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.opensource.org/licenses/eclipse-1.0.php
 */
package examples.utsThreeArrays;

import static apgas.Constructs.asyncAny;
import static apgas.Constructs.getThreadLocalResult;
import static apgas.Constructs.setThreadLocalResult;

import java.io.Serializable;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class UTS implements Serializable {

  private static final long serialVersionUID = 2200935927036145803L;

  // branching factor: 4
  private static final double DEN = Math.log(4.0 / (1.0 + 4.0));

  private static final MessageDigest[] mdList = new MessageDigest[1024];

  public static void initMdList() {
    for (int i = 0; i < mdList.length; i++) {
      try {
        mdList[i] = MessageDigest.getInstance("SHA-1");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
    }
  }

  private static MessageDigest getMD() {
    int id = (int) Thread.currentThread().getId();
    return mdList[id];
  }

  private byte[] hash;
  private int[] depth;
  private int[] lower;
  private int[] upper;
  private int size; // number of nodes in the bag
  private long count; // number of nodes processed so far

  public UTS() {}

  public UTS(int n) {
    hash = new byte[n * 20 + 4]; // slack for in-place SHA1 computation
    depth = new int[n];
    lower = new int[n];
    upper = new int[n];
  }

  private void digest(MessageDigest md, int d) throws DigestException {
    if (size >= depth.length) {
      grow();
    }
    ++count;
    final int offset = size * 20;
    md.digest(hash, offset, 20);
    final int v =
        ((0x7f & hash[offset + 16]) << 24)
            | ((0xff & hash[offset + 17]) << 16)
            | ((0xff & hash[offset + 18]) << 8)
            | (0xff & hash[offset + 19]);
    final int n = (int) (Math.log(1.0 - v / 2147483648.0) / DEN);
    if (n > 0) {
      if (d > 1) {
        depth[size] = d - 1;
        lower[size] = 0;
        upper[size++] = n;
      } else {
        count += n;
      }
    }
  }

  public void seed(int s, int d) {
    try {
      for (int i = 0; i < 16; ++i) {
        hash[i] = 0;
      }
      hash[16] = (byte) (s >> 24);
      hash[17] = (byte) (s >> 16);
      hash[18] = (byte) (s >> 8);
      hash[19] = (byte) s;
      MessageDigest md = getMD();
      md.update(hash, 0, 20);
      digest(md, d);
    } catch (final DigestException e) {
    }
  }

  private void expand() throws DigestException {
    final int top = size - 1;
    final int d = depth[top];
    final int l = lower[top];
    final int u = upper[top] - 1;
    if (u == l) {
      size = top;
    } else {
      upper[top] = u;
    }
    final int offset = top * 20;
    hash[offset + 20] = (byte) (u >> 24);
    hash[offset + 21] = (byte) (u >> 16);
    hash[offset + 22] = (byte) (u >> 8);
    hash[offset + 23] = (byte) u;
    MessageDigest md = getMD();
    md.update(hash, offset, 24);
    digest(md, d);
  }

  public void run() {
    try {
      while (size > 0) {
        for (int n = 511; (n > 0) && (size > 0); --n) {
          expand();
        }
        final UTS split = split();
        if (split != null) {
          asyncAny(split::run);
        }
      }
    } catch (final DigestException e) {
    }

    addResult();
  }

  public UTS trim() {
    final UTS b;
    if (size == 0) {
      b = new UTS();
    } else {
      b = new UTS(size);
      System.arraycopy(hash, 0, b.hash, 0, size * 20);
      System.arraycopy(depth, 0, b.depth, 0, size);
      System.arraycopy(lower, 0, b.lower, 0, size);
      System.arraycopy(upper, 0, b.upper, 0, size);
      b.size = size;
    }
    return b;
  }

  private UTS split() {
    int s = 0;
    for (int i = 0; i < size; ++i) {
      if ((upper[i] - lower[i]) >= 2) {
        ++s;
      }
    }
    if (s == 0) {
      return null;
    }
    final UTS b = new UTS(s);
    for (int i = 0; i < size; ++i) {
      final int p = upper[i] - lower[i];
      if (p >= 2) {
        System.arraycopy(hash, i * 20, b.hash, b.size * 20, 20);
        b.depth[b.size] = depth[i];
        b.upper[b.size] = upper[i];
        b.lower[b.size++] = upper[i] -= p / 2;
      }
    }
    return b;
  }

  public void merge(UTS b) {
    final int s = size + b.size;
    while (s > depth.length) {
      grow();
    }
    System.arraycopy(b.hash, 0, hash, size * 20, b.size * 20);
    System.arraycopy(b.depth, 0, depth, size, b.size);
    System.arraycopy(b.lower, 0, lower, size, b.size);
    System.arraycopy(b.upper, 0, upper, size, b.size);
    size = s;
  }

  private void grow() {
    final int n = depth.length * 2;
    final byte[] h = new byte[n * 20 + 4];
    final int[] d = new int[n];
    final int[] l = new int[n];
    final int[] u = new int[n];
    System.arraycopy(hash, 0, h, 0, size * 20);
    System.arraycopy(depth, 0, d, 0, size);
    System.arraycopy(lower, 0, l, 0, size);
    System.arraycopy(upper, 0, u, 0, size);
    hash = h;
    depth = d;
    lower = l;
    upper = u;
  }

  private static String sub(String str, int start, int end) {
    return str.substring(start, Math.min(end, str.length()));
  }

  private void addResult() {
    UTSResult result = getThreadLocalResult();
    if (result == null) {
      result = new UTSResult();
      setThreadLocalResult(result);
    }
    result.mergeResult(this.count);
  }

  public static void main(String[] args) {
    int depth = 13;
    try {
      depth = Integer.parseInt(args[0]);
    } catch (final Exception e) {
    }

    initMdList();

    UTS bag = new UTS(64);

    System.out.println("Warmup...");
    bag.seed(19, depth - 2);
    bag.run();

    bag = new UTS(64);

    System.out.println("Starting...");
    long time = System.nanoTime();

    bag.seed(19, depth);
    bag.run();

    time = System.nanoTime() - time;
    System.out.println("Finished.");

    final long count = bag.count;
    System.out.println(
        "Depth: "
            + depth
            + ", Performance: "
            + count
            + "/"
            + sub("" + time / 1e9, 0, 6)
            + " = "
            + sub("" + (count / (time / 1e3)), 0, 6)
            + "M nodes/s");
  }
}
