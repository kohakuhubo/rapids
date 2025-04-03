package cn.berry.rapids.data;

import cn.berry.rapids.Stoppable;

/**
 * 可执行类
 * 
 * 描述: 定义可执行项的基本行为，提供可中断的执行能力。
 * 
 * 特性:
 * 1. 继承自可停止类
 * 2. 实现可运行接口
 * 
 * @author Berry
 * @version 1.0.0
 */
public abstract class Executable extends Stoppable implements Runnable {
}
