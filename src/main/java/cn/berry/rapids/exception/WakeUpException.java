package cn.berry.rapids.exception;

public class WakeUpException extends RuntimeException {

    public WakeUpException() {
    }

    public WakeUpException(String message) {
        super(message);
    }

    public WakeUpException(String message, Throwable cause) {
        super(message, cause);
    }

    public WakeUpException(Throwable cause) {
        super(cause);
    }

    public WakeUpException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
