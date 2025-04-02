package cn.berry.rapids;

import picocli.CommandLine;

/**
 * 数据计算引导类
 * 
 * 描述: 应用程序的入口点，负责解析命令行参数并启动应用服务器。
 * 
 * 特性:
 * 1. 使用picocli框架解析命令行参数
 * 2. 提供帮助和版本信息
 * 
 * @author Berry
 * @version 1.0.0
 */
public class DataCalculationBootstrap {

    /**
     * 主方法，程序的入口
     * 
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        final AppServerBootstrapCommand command = new AppServerBootstrapCommand();
        final CommandLine commandLine = new CommandLine(command);
        try {
            final CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
            checkParamHelp(commandLine, parseResult);
            commandLine.execute(args);
        } catch (CommandLine.ParameterException e) {
            commandLine.usage(System.out);
            for (CommandLine c : commandLine.getSubcommands().values())
                c.usage(System.out);
            System.exit(CommandConstant.ERROR);
        }
    }

    /**
     * 检查参数帮助请求
     * 
     * @param commandLine 命令行对象
     * @param parseResult 解析结果
     */
    private static void checkParamHelp(CommandLine commandLine, CommandLine.ParseResult parseResult) {
        if (parseResult.isUsageHelpRequested()) {
            commandLine.usage(System.out);
            System.exit(CommandConstant.CANCEL);
        }
        if (parseResult.isVersionHelpRequested()) {
            commandLine.printVersionHelp(System.out);
            System.exit(CommandConstant.CANCEL);
        }
    }
}