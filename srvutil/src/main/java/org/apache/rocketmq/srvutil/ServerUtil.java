package org.apache.rocketmq.srvutil;

import org.apache.commons.cli.*;

import java.util.Properties;

/**
 * 命令行工具类
 */
public class ServerUtil {

    /**
     * 构建默认options
     */
    public static Options buildCommandlineOptions(Options options) {
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("n", "namesrvAddr", true, "Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    /**
     * 将命令行参数解析成CommandLine
     */
    public static CommandLine parseCmdLine(String appName, String[] args, Options options, CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                System.exit(0);
            }
        } catch (ParseException e) {
            hf.printHelp(appName, options, true);
            System.exit(1);
        }
        return commandLine;
    }

    /**
     * 打印命令帮助
     */
    public static void printCommandLineHelp(String appName, Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    /**
     * 命令行参数longFlag转换成properties
     */
    public static Properties commandLine2Properties(CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            for (Option opt : opts) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}
