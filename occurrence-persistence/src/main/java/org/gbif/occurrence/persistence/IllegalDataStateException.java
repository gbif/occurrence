package org.gbif.occurrence.persistence;

/**
 * Thrown to indicate that an issue in the state of data prevents a persistence operation.
 * <p/>
 * When handling this exception clients should either clean data or fail gracefully.  Retrying the operation without
 * addressing the root cause is expected to yield the same exception.
 * <p/>
 * Note: The persistence API is designed exclusively around RuntimeExceptions and therefore this respects that, despite
 * being an exception that would typically be checked - it offers clients the possibility to code for recovery.
 */
public class IllegalDataStateException extends RuntimeException {
  public IllegalDataStateException(String message) {
    super(message);
  }
}
