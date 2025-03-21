package cn.berry.rapids;

import picocli.CommandLine;

public class DataCalculationBootstrap {

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
