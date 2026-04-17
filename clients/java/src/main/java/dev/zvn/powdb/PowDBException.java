package dev.zvn.powdb;

/** Raised when the server returns an error, the client is in a bad state, or an unexpected frame arrives. */
public class PowDBException extends RuntimeException {
    public PowDBException(String message) {
        super(message);
    }

    public PowDBException(String message, Throwable cause) {
        super(message, cause);
    }
}
