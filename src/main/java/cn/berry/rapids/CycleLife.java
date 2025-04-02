package cn.berry.rapids;

/**
 * 生命周期接口
 * 
 * 描述: 定义组件生命周期管理的基本方法，包括启动和停止。
 * 实现此接口的类需要提供组件的启动和停止功能，确保资源的正确初始化和释放。
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface CycleLife {

    /**
     * 启动组件
     * 
     * 详细描述: 初始化资源并启动组件，使其进入可服务状态。
     * 实现类应在此方法中完成所有必要的初始化工作。
     * 
     * @throws Exception 启动过程中可能发生的任何异常
     */
    void start() throws Exception;

    /**
     * 停止组件
     * 
     * 详细描述: 停止组件并释放资源，使其进入不可服务状态。
     * 实现类应在此方法中完成所有必要的资源释放工作，确保不发生资源泄漏。
     * 
     * @throws Exception 停止过程中可能发生的任何异常
     */
    void stop() throws Exception;

}
