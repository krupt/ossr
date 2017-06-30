package ru.krupt.ossr.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

public class SqlCommentsUtils {

    public static final String SINGLE_LINE_COMMENT = "--";
    public static final String MULTI_LINE_COMMENT_START = "/*";
    public static final String MULTI_LINE_COMMENT_END = "*/";

    @SuppressWarnings("WeakerAccess")
    public static String skipSqlComments(String source) {
        return skipSqlComments(source, 0);
    }

    public static String skipSqlComments(String source, int linesLimit) {
        final StringBuilder builder = new StringBuilder(32768);
        int linesAdded = 0;
        int lines = 0;
        boolean multiLineCommentStarted = false;
        try (final BufferedReader reader = new BufferedReader(new StringReader(source))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (linesLimit > 0 && ++lines > linesLimit) {
                    break;
                }
                final String trimmedLine = line.trim();
                if (StringUtils.isNotEmpty(trimmedLine)) {
                    if (!trimmedLine.startsWith(SINGLE_LINE_COMMENT)) {
                        final int multiLineCommentStartPosition = trimmedLine.indexOf(MULTI_LINE_COMMENT_START);
                        if (!multiLineCommentStarted && multiLineCommentStartPosition >= 0) {
                            final int multiLineCommentEndPosition = trimmedLine.indexOf(MULTI_LINE_COMMENT_END);
                            if (multiLineCommentEndPosition >= 0) {
                                line = trimmedLine.substring(0, multiLineCommentStartPosition)
                                        + trimmedLine.substring(multiLineCommentEndPosition + MULTI_LINE_COMMENT_END.length());
                            } else {
                                multiLineCommentStarted = true;
                                line = trimmedLine.substring(0, multiLineCommentStartPosition);
                            }
                        } else if (multiLineCommentStarted) {
                            final int multiLineCommentEndPosition = trimmedLine.indexOf(MULTI_LINE_COMMENT_END);
                            if (multiLineCommentEndPosition >= 0) {
                                multiLineCommentStarted = false;
                                line = trimmedLine.substring(multiLineCommentEndPosition + MULTI_LINE_COMMENT_END.length());
                            } else {
                                continue;
                            }
                        }
                        if (StringUtils.isNotEmpty(line)) {
                            builder.append(linesAdded++ == 0 ? "" : " ").append(line);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return builder.toString();
    }
}
