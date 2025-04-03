package cn.berry.rapids.exception;

/**
 * 业务异常类
 * 
 * 描述: 表示业务逻辑相关的异常，用于处理业务层面的错误情况。
 * 
 * 特性:
 * 1. 继承自RuntimeException
 * 2. 提供无参构造函数
 * 3. 提供带消息的构造函数
 * 
 * @author Berry
 * @version 1.0.0
 */
public class BusinessException extends RuntimeException {

    /**
     * 构造无参业务异常
     */
    public BusinessException() {
        super();
    }

    /**
     * 构造带消息的业务异常
     * 
     * @param message 异常消息
     */
    public BusinessException(String message) {
        super(message);
    }

    public BusinessException(String message, Throwable cause) {
        super(message, cause);
    }

    public BusinessException(Throwable cause) {
        super(cause);
    }

    public BusinessException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
