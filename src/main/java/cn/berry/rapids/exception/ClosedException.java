package cn.berry.rapids.exception;

/**
 * 关闭异常类
 * 
 * 描述: 表示资源或服务已关闭的异常，用于处理关闭状态下的操作请求。
 * 
 * 特性:
 * 1. 继承自RuntimeException
 * 2. 提供无参构造函数
 * 3. 提供带消息的构造函数
 * 
 * @author Berry
 * @version 1.0.0
 */
public class ClosedException extends RuntimeException {

    /**
     * 构造无参关闭异常
     */
    public ClosedException() {
        super();
    }

    /**
     * 构造带消息的关闭异常
     * 
     * @param message 异常消息
     */
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
