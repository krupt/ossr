package ru.krupt.ossr.script;

import ru.krupt.ossr.util.SqlCommentsUtils;
import ru.krupt.ossr.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;

public class ScriptParser {

    public static Iterable<Command> parseFile(InputStream input) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, "windows-1251"))) {
            Collection<Command> commands = new ArrayList<>();
            String line;
            StringBuilder commandText = new StringBuilder(32768);
            CommandType currentCommandType = CommandTypes.DEFAULT;
            boolean multiLineCommentStarted = false;
            while ((line = reader.readLine()) != null) {
                final String trimmedCommand = commandText.toString().toLowerCase().trim();
                if (currentCommandType == CommandTypes.DEFAULT) {
                    currentCommandType = CommandTypeFactory.getCommand(trimmedCommand);
                }
                String trimmedLine = line.trim();
                boolean skipParseOnLine = false;
                if (currentCommandType == CommandTypes.DEFAULT) {
                    final int singleLineCommentStartPosition = trimmedLine.indexOf(SqlCommentsUtils.SINGLE_LINE_COMMENT);
                    if (!multiLineCommentStarted && singleLineCommentStartPosition >= 0) {
                        if (singleLineCommentStartPosition == 0) {
                            skipParseOnLine = true;
                        } else {
                            trimmedLine = trimmedLine.substring(0, singleLineCommentStartPosition);
                        }
                    } else {
                        final int multiLineCommentStartPosition = trimmedLine.indexOf(SqlCommentsUtils.MULTI_LINE_COMMENT_START);
                        if (!multiLineCommentStarted && multiLineCommentStartPosition >= 0) {
                            final int multiLineCommentEndPosition = trimmedLine.indexOf(SqlCommentsUtils.MULTI_LINE_COMMENT_END);
                            if (multiLineCommentEndPosition < 0) {
                                multiLineCommentStarted = true;
                                if (multiLineCommentStartPosition == 0) {
                                    skipParseOnLine = true;
                                } else {
                                    trimmedLine = trimmedLine.substring(0, multiLineCommentStartPosition).trim();
                                }
                            } else {
                                trimmedLine = (trimmedLine.substring(0, multiLineCommentStartPosition)
                                        + trimmedLine.substring(multiLineCommentEndPosition + SqlCommentsUtils.MULTI_LINE_COMMENT_END.length()))
                                        .trim();
                            }
                        } else if (multiLineCommentStarted) {
                            final int multiLineCommentEndPosition = trimmedLine.indexOf(SqlCommentsUtils.MULTI_LINE_COMMENT_END);
                            if (multiLineCommentEndPosition >= 0) {
                                multiLineCommentStarted = false;
                                if (multiLineCommentEndPosition == trimmedLine.length() - 1 - SqlCommentsUtils.MULTI_LINE_COMMENT_END.length()) {
                                    skipParseOnLine = true;
                                } else {
                                    trimmedLine = trimmedLine.substring(multiLineCommentEndPosition
                                            + SqlCommentsUtils.MULTI_LINE_COMMENT_END.length()).trim();
                                }
                            } else {
                                skipParseOnLine = true;
                            }
                        }
                    }
                }
                if (StringUtils.isNotEmpty(trimmedLine) || StringUtils.isNotEmpty(trimmedCommand)) {
                    final boolean terminateCharEquals = currentCommandType.isTerminateCharEquals();
                    final char terminateChar = currentCommandType.getTerminateChar();
                    if (!skipParseOnLine && ((terminateCharEquals && StringUtils.equals(trimmedLine, terminateChar))
                            || (!terminateCharEquals && trimmedLine.indexOf(terminateChar) >= 0))) {
                        if (!terminateCharEquals) {
                            addNewLineIfNeeded(commandText);
                            commandText.append(line, 0, line.indexOf(trimmedLine) + trimmedLine.indexOf(terminateChar));
                        }
                        commands.add(CommandFactory.instantiateCommand(commandText.toString(), currentCommandType));
                        commandText = new StringBuilder(32768);
                        currentCommandType = CommandTypes.DEFAULT;
                    } else {
                        addNewLineIfNeeded(commandText);
                        commandText.append(line);
                    }
                }
            }
            return commands;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static void addNewLineIfNeeded(StringBuilder builder) {
        if (builder.length() != 0) {
            builder.append("\r\n");
        }
    }
}
