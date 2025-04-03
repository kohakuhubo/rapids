package cn.berry.rapids.exception;

/**
 * 唤醒异常类
 * 
 * 描述: 表示需要唤醒线程的异常，用于控制线程的执行流程。
 * 
 * 特性:
 * 1. 继承自RuntimeException
 * 2. 提供无参构造函数
 * 3. 提供带消息的构造函数
 * 
 * @author Berry
 * @version 1.0.0
 */
public class WakeUpException extends RuntimeException {

    /**
     * 构造无参唤醒异常
     */
    public WakeUpException() {
        super();
    }

    /**
     * 构造带消息的唤醒异常
     * 
     * @param message 异常消息
     */
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
