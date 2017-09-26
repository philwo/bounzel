package com.google.devtools.build.lib.boundary;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.function.Predicate;

public class BoundaryRetrier extends Retrier {

  private static final Predicate<? super Exception> RETRIABLE_ERRORS =
      e -> {
        if (!(e instanceof StatusException) && !(e instanceof StatusRuntimeException)) {
          return false;
        }
        Status s = Status.fromThrowable(e);
        switch (s.getCode()) {
          case CANCELLED:
            return !Thread.currentThread().isInterrupted();
          case UNKNOWN:
          case DEADLINE_EXCEEDED:
          case ABORTED:
          case INTERNAL:
          case UNAVAILABLE:
          case UNAUTHENTICATED:
            return true;
          default:
            return false;
        }
      };

  private static final class SimpleBackoff implements Backoff {

    private final long delayMillis;
    private final int maxRetries;

    private int attempts;

    SimpleBackoff(long delayMillis, int maxRetries) {
      this.delayMillis = delayMillis;
      this.maxRetries = maxRetries;
    }

    @Override
    public long nextDelayMillis() {
      if (attempts == maxRetries) {
        return -1;
      }
      attempts++;
      return delayMillis;
    }

    @Override
    public int getRetryAttempts() {
      return attempts;
    }
  }

  public BoundaryRetrier(long delayMillis, int maxRetries) {
    super(() -> new SimpleBackoff(delayMillis, maxRetries), RETRIABLE_ERRORS,
        Retrier.ALLOW_ALL_CALLS);
  }
}
