package cn.berry.rapids;

/**
 * 可停止组件基类
 * 
 * 描述: 提供组件停止功能的基础实现，维护组件的停止状态。
 * 此类可以作为需要实现停止功能的组件的基类，提供统一的状态管理。
 * 
 * 特性:
 * 1. 使用volatile保证停止标志在多线程环境下的可见性
 * 2. 提供简单的状态查询和改变方法
 * 
 * @author Berry
 * @version 1.0.0
 */
public class Stoppable {

    /** 停止标志，volatile保证多线程可见性 */
    protected volatile boolean stopped = false;

    /**
     * 停止组件
     * 
     * 详细描述: 将组件标记为已停止状态。
     * 子类可以重写此方法以添加额外的停止逻辑，但通常应调用super.stop()。
     */
    public void stop() {
        this.stopped = true;
    }

    /**
     * 检查组件是否已终止
     * 
     * 详细描述: 返回组件的当前停止状态。
     * 
     * @return true表示组件已停止，false表示组件仍在运行
     */
    public boolean isTerminal() {
        return this.stopped;
    }
}
