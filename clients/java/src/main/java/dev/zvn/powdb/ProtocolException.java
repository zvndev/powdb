package dev.zvn.powdb;

/** Raised when a wire frame is malformed. */
public class ProtocolException extends RuntimeException {
    public ProtocolException(String message) {
        super(message);
    }
}
