package IncFTGLB;

import java.io.Serializable;

public interface IncTaskBag extends Serializable {

  int size();

  void mergeAtTop(IncTaskBag bag);
}
