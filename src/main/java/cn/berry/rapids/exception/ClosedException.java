package cn.berry.rapids.exception;

public class ClosedException extends RuntimeException {
    public ClosedException() {
    }

    public ClosedException(String message) {
        super(message);
    }

    public ClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClosedException(Throwable cause) {
        super(cause);
    }

    public ClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
