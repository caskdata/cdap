/*
 * Copyright © 2012-2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.cli;

import co.cask.cdap.cli.command.system.HelpCommand;
import co.cask.cdap.cli.command.system.SearchCommandsCommand;
import co.cask.cdap.cli.commandset.DefaultCommands;
import co.cask.cdap.cli.completer.supplier.EndpointSupplier;
import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.cli.util.table.AltStyleTableRenderer;
import co.cask.cdap.cli.util.table.TableRenderer;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.exception.DisconnectedException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.common.cli.CLI;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import co.cask.common.cli.exception.CLIExceptionHandler;
import co.cask.common.cli.exception.InvalidCommandException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import jline.console.completer.Completer;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;
import javax.net.ssl.SSLHandshakeException;

/**
 * Main class for the CDAP CLI.
 */
public class CLIMain {

  private static final boolean DEFAULT_VERIFY_SSL = true;
  private static final boolean DEFAULT_AUTOCONNECT = true;
  private static final String DEFAULT_SCRIPT_FILE = null;
  private static final boolean DEFAULT_SCRIPT_FILE_FIRST = false;

  @VisibleForTesting
  public static final Option HELP_OPTION = new Option(
    "h", "help", false, "Print the usage message.");

  @VisibleForTesting
  public static final Option URI_OPTION = new Option(
    "u", "uri", true, "CDAP instance URI to interact with in" +
    " the format \"[http[s]://]<hostname>[:<port>[/<namespace>]]\"." +
    " Defaults to \"" + getDefaultURI().toString() + "\".");

  @VisibleForTesting
  public static final Option VERIFY_SSL_OPTION = new Option(
    "s", "verify-ssl", true, "If \"true\", verify SSL certificate when making requests." +
    " Defaults to \"" + DEFAULT_VERIFY_SSL + "\".");

  @VisibleForTesting
  public static final Option AUTOCONNECT_OPTION = new Option(
    "a", "autoconnect", true, "If \"true\", try provided connection" +
    " (from " + URI_OPTION.getLongOpt() + ")" +
    " upon launch or try default connection if none provided." +
    " Defaults to \"" + DEFAULT_AUTOCONNECT + "\".");

  @VisibleForTesting
  public static final Option DEBUG_OPTION = new Option(
    "d", "debug", false, "Print exception stack traces.");

  @VisibleForTesting
  public static final Option SCRIPT_FILE_OPTION = new Option(
    "f", "scriptfile", true, 
    "Name of script file to read and execute.  Blank lines, " + 
    "and comments starting with '#' are ignored.  By default the " + 
    "cdap-cli shell terminates when the script finishes.  " + 
    "A 'break' command causes the script to terminate early and leaves " +
    "the user at the cdap-cli command prompt.  Commands " + 
    "added after the script file specification on the command line " + 
    "are run after the script completes, unless a 'break' " + 
    "command is encountered, then they are skipped. ");

  @VisibleForTesting
  public static final Option SCRIPT_FILE_FIRST_OPTION = new Option(
    "ff", "scriptfilefirst", false, "If \"true\", run script file before " +
    "any command line commands are run." +
    " Defaults to \"" + DEFAULT_SCRIPT_FILE_FIRST + "\".");

  private final CLI cli;
  private final Iterable<CommandSet<Command>> commands;
  private final CLIConfig cliConfig;
  private final Injector injector;
  private final LaunchOptions options;

  public CLIMain(final LaunchOptions options, final CLIConfig cliConfig) throws URISyntaxException, IOException {
    this.options = options;
    this.cliConfig = cliConfig;

    cliConfig.getClientConfig().setVerifySSLCert(options.isVerifySSL());
    injector = Guice.createInjector(
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(LaunchOptions.class).toInstance(options);
          bind(CConfiguration.class).toInstance(CConfiguration.create());
          bind(PrintStream.class).toInstance(cliConfig.getOutput());
          bind(CLIConfig.class).toInstance(cliConfig);
          bind(ClientConfig.class).toInstance(cliConfig.getClientConfig());
        }
      }
    );

    this.commands = ImmutableList.of(
      injector.getInstance(DefaultCommands.class),
      new CommandSet<Command>(ImmutableList.<Command>of(
        new HelpCommand(getCommandsSupplier(), cliConfig),
        new SearchCommandsCommand(getCommandsSupplier(), cliConfig)
      )));
    Map<String, Completer> completers = injector.getInstance(DefaultCompleters.class).get();
    cli = new CLI<Command>(Iterables.concat(commands), completers);
    cli.setExceptionHandler(new CLIExceptionHandler<Exception>() {
      @Override
      public boolean handleException(PrintStream output, Exception e, int timesRetried) {
        if (e instanceof SSLHandshakeException) {
          output.printf("To ignore this error, set \"--%s false\" when starting the CLI\n",
                        VERIFY_SSL_OPTION.getLongOpt());
        } else if (e instanceof InvalidCommandException) {
          InvalidCommandException ex = (InvalidCommandException) e;
          output.printf("Invalid command '%s'. Enter 'help' for a list of commands\n", ex.getInput());
        } else if (e instanceof DisconnectedException) {
          cli.getReader().setPrompt("cdap (DISCONNECTED)> ");
        } else {
          output.println("Error: " + e.getMessage());
        }

        if (options.isDebug()) {
          e.printStackTrace(output);
        }

        return false;
      }
    });
    cli.addCompleterSupplier(injector.getInstance(EndpointSupplier.class));
    cli.getReader().setExpandEvents(false);
    cliConfig.addHostnameChangeListener(new CLIConfig.ConnectionChangeListener() {
      @Override
      public void onConnectionChanged(ClientConfig clientConfig) {
        updateCLIPrompt(clientConfig);
      }
    });
  }

  /**
   * Tries to autoconnect to the provided URI in options.
   */
  public void tryAutoconnect() {
    InstanceURIParser instanceURIParser = injector.getInstance(InstanceURIParser.class);
    if (options.isAutoconnect()) {
      try {
        ConnectionConfig connectionInfo = instanceURIParser.parse(options.getUri());
        cliConfig.tryConnect(connectionInfo, cliConfig.getOutput(), options.isDebug());
      } catch (Exception e) {
        if (options.isDebug()) {
          e.printStackTrace(cliConfig.getOutput());
        }
      }
    }
  }

  public static URI getDefaultURI() {
    return ConnectionConfig.DEFAULT.getURI();
  }

  private String limit(String string, int maxLength) {
    if (string.length() <= maxLength) {
      return string;
    }

    if (string.length() >= 4) {
      return string.substring(0, string.length() - 3) + "...";
    } else {
      return string;
    }
  }

  private void updateCLIPrompt(ClientConfig clientConfig) {
    try {
      ConnectionConfig connectionConfig = clientConfig.getConnectionConfig();
      URI baseURI = connectionConfig.getURI();
      URI uri = baseURI.resolve("/" + connectionConfig.getNamespace());
      cli.getReader().setPrompt("cdap (" + uri + ")> ");
    } catch (DisconnectedException e) {
      cli.getReader().setPrompt("cdap (DISCONNECTED)> ");
    }
  }

  public TableRenderer getTableRenderer() {
    return cliConfig.getTableRenderer();
  }

  public CLI getCLI() {
    return this.cli;
  }

  public Supplier<Iterable<CommandSet<Command>>> getCommandsSupplier() {
    return new Supplier<Iterable<CommandSet<Command>>>() {
      @Override
      public Iterable<CommandSet<Command>> get() {
        return commands;
      }
    };
  }

  public static void main(String[] args) {
    final PrintStream output = System.out;

    Options options = getOptions();
    CLIMainArgs cliMainArgs = CLIMainArgs.parse(args, options);

    CommandLineParser parser = new BasicParser();
    try {
      CommandLine command = parser.parse(options, cliMainArgs.getOptionTokens());
      if (command.hasOption(HELP_OPTION.getOpt())) {
        usage();
        System.exit(0);
      }

      LaunchOptions launchOptions = LaunchOptions.builder()
        .setUri(command.getOptionValue(URI_OPTION.getOpt(), getDefaultURI().toString()))
        .setScriptFile(command.getOptionValue(SCRIPT_FILE_OPTION.getOpt(), DEFAULT_SCRIPT_FILE))
        .setScriptFileFirst(command.hasOption(SCRIPT_FILE_FIRST_OPTION.getOpt()))
        .setDebug(command.hasOption(DEBUG_OPTION.getOpt()))
        .setVerifySSL(parseBooleanOption(command, VERIFY_SSL_OPTION, DEFAULT_VERIFY_SSL))
        .setAutoconnect(parseBooleanOption(command, AUTOCONNECT_OPTION, DEFAULT_AUTOCONNECT))
        .build();

      String[] commandArgs = cliMainArgs.getCommandTokens();

      try {
        ClientConfig clientConfig = ClientConfig.builder().setConnectionConfig(null).build();
        final CLIConfig cliConfig = new CLIConfig(clientConfig, output, new AltStyleTableRenderer());
        CLIMain cliMain = new CLIMain(launchOptions, cliConfig);
        CLI cli = cliMain.getCLI();

        cliMain.tryAutoconnect();
        cliMain.updateCLIPrompt(cliConfig.getClientConfig());

        ConnectionConfig connectionConfig = clientConfig.getConnectionConfig();
        URI baseURI = connectionConfig.getURI();
        URI uri = baseURI.resolve("/" + connectionConfig.getNamespace());
        String cliPrompt = "\ncdap (" + uri + ")> ";

        // Execute commands entered on cdap-cli command line
        boolean runScriptFileFirst = launchOptions.getScriptFileFirst();
        if (commandArgs.length > 0 && !runScriptFileFirst) {
          ArrayList<String> cmds = parseCmdStr(Joiner.on(" ").join(commandArgs));
          for (String cmd : cmds) {
            output.println(cliPrompt + cmd);
            cli.execute(cmd, output);
          }
        }

        boolean ranScript = false;
        boolean brokeFromScript = false;
        if (launchOptions.getScriptFile() != null) {

          ranScript = true;

          BufferedReader br = null;
          try {
            String line;
            
            br = Files.newReader(new File(launchOptions.getScriptFile()), Charsets.UTF_8);
            while ((line = br.readLine()) != null) {
              ArrayList<String> cmds = parseCmdStr(line);
              for (String cmd : cmds) {
                if (cmd.equals("break")) {
                  brokeFromScript = true;
                  break;
                }
                if (brokeFromScript) {
                  break; // Break from script processing and drop into interactive mode
                }
                output.println(cliPrompt + cmd);
                cli.execute(cmd, output);
              }
            }
            output.println("\n");
          } catch (IOException e) {
            output.println("Couldn't read from script '" + 
                           launchOptions.getScriptFile() + "'\n" + e.getMessage());
          } finally {
            try {
              if (br != null) {
                br.close();
              }
            } catch (IOException e) {
              output.println("Couldn't close script '" + 
                             launchOptions.getScriptFile() + "'\n" + e.getMessage());
            }
          }
        }

        // Execute commands entered on cdap-cli command line
        if (commandArgs.length > 0 && runScriptFileFirst) {
          ArrayList<String> cmds = parseCmdStr(Joiner.on(" ").join(commandArgs));
          for (String cmd : cmds) {
            output.println(cliPrompt + cmd);
            cli.execute(cmd, output);
          }
        }

        // Enter interactive cdap-cli shell
        if ((commandArgs.length == 0 && !ranScript) || brokeFromScript) {
          cli.startInteractiveMode(output);
        }
      } catch (Exception e) {
        e.printStackTrace(output);
      }
    } catch (ParseException e) {
      output.println(e.getMessage());
      usage();
    }
  }

  private static ArrayList<String> parseCmdStr(String str) {
    ArrayList<String> cmds = new ArrayList<String>();
    boolean isEscaped = false;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean isMulti = false;
    for (int i = 0; i < str.length(); ++i) {
      if (str.charAt(i) == '"' && isEscaped) {
        str = str.substring(0, i - 1) + str.substring(i);
        i -= 1;
        continue;
      }
      if (str.charAt(i) == '\'' && isEscaped) {
        str = str.substring(0, i - 1) + str.substring(i);
        i -= 1;
        continue;
      }
      if (str.charAt(i) == '\'' && !inDoubleQuote) {
        inSingleQuote = !inSingleQuote;
        continue;
      }
      if (inSingleQuote) {
        continue;
      }
      if (str.charAt(i) == '"' && !inSingleQuote) {
        inDoubleQuote = !inDoubleQuote;
        continue;
      }
      if (inDoubleQuote) {
        continue;
      }
      if (str.charAt(i) == '\\') {
        isEscaped = !isEscaped;
        continue;
      }
      if (str.charAt(i) == '#' && isEscaped) {
        str = str.substring(0, i - 1) + str.substring(i);
        i -= 1;
      }
      if (str.charAt(i) == '#' && !isEscaped && !inSingleQuote && !inDoubleQuote) {
        str = str.substring(0, i);
        isEscaped = false;
        break;
      }
      if (str.charAt(i) == ';' && isEscaped) {
        str = str.substring(0, i - 1) + str.substring(i);
        i -= 1;
        continue;
      }
      if (str.charAt(i) == ';' && !isEscaped && !inSingleQuote && !inDoubleQuote) {
        String str1 = str.substring(0, i).trim();
        String str2;
        if (str.length() > i + 1) {
          str2 = str.substring(i + 1).trim();
        } else {
          str2 = "";
        }
        if (!"".equals(str1)) {
          cmds.add(str1);
        }
        if (!"".equals(str2)) {
          ArrayList<String> subCmds = parseCmdStr(str2);
          for (String subStr : subCmds) {
            String str3 = subStr.trim();
            if (!"".equals(str3)) {
              cmds.add(str3);
            }
          }
        }
        isMulti = true;
        break;
      }
      isEscaped = false;
    }
    if (!isMulti) {
      String str4 = str.trim();
      if (!"".equals(str4)) {
        cmds.add(str4);
      }
    }
    return cmds;
  }

  private static boolean parseBooleanOption(CommandLine command, Option option, boolean defaultValue) {
    String value = command.getOptionValue(option.getOpt(), Boolean.toString(defaultValue));
    return "true".equals(value);
  }

  @VisibleForTesting
  public static Options getOptions() {
    Options options = new Options();
    addOptionalOption(options, HELP_OPTION);
    addOptionalOption(options, URI_OPTION);
    addOptionalOption(options, VERIFY_SSL_OPTION);
    addOptionalOption(options, AUTOCONNECT_OPTION);
    addOptionalOption(options, DEBUG_OPTION);
    addOptionalOption(options, SCRIPT_FILE_OPTION);
    addOptionalOption(options, SCRIPT_FILE_FIRST_OPTION);
    return options;
  }

  private static void addOptionalOption(Options options, Option option) {
    OptionGroup optionalGroup = new OptionGroup();
    optionalGroup.setRequired(false);
    optionalGroup.addOption(option);
    options.addOptionGroup(optionalGroup);
  }

  private static void usage() {
    HelpFormatter formatter = new HelpFormatter();
    String args =
      "[--autoconnect <true|false>] " +
      "[--debug] " +
      "[--help] " +
      "[--verify-ssl <true|false>] " +
      "[--uri <arg>]" +
      "[--scriptfile <arg>]" +
      "[--scriptfilefirst]";
    formatter.printHelp("cdap-cli.sh " + args, getOptions());
    System.exit(0);
  }

}
