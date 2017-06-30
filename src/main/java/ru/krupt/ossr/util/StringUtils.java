package ru.krupt.ossr.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StringUtils {

    public static boolean endsWith(String source, char symbol) {
        final int position = source.length() - 1;
        return position > 0 && source.charAt(position) == symbol;
    }

    public static boolean equals(String source, char symbol) {
        return source.length() == 1 && source.charAt(0) == symbol;
    }

    @SuppressWarnings("WeakerAccess")
    public static boolean isNotEmpty(String source) {
        return source != null && source.length() != 0;
    }

    public static String toSingleSpaces(String source) {
        return source.replaceAll(" +", " ");
    }

    public static String clipWithEllipsis(String source) {
        return clipWithEllipsis(source, 125);
    }

    @SuppressWarnings("WeakerAccess")
    public static String clipWithEllipsis(String source, int length) {
        if (source.length() > length) {
            return source.substring(0, length) + "...";
        }
        return source;
    }

    public static String withoutLines(String source) {
        return source.replaceAll("(\\r|\\n|\\r\\n)+", " ");
    }

    public static String capitalize(String source) {
        return source.substring(0, 1).toUpperCase() + source.substring(1).toLowerCase();
    }

    public static boolean contains(String source, char symbol) {
        final char[] chars = source.toCharArray();
        for (char sourceChar : chars) {
            if (sourceChar == symbol) {
                return true;
            }
        }
        return false;
    }
}
