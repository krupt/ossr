package ru.krupt.ossr.schema;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SchemaUtils {

    private static final char DEFAULT_SCHEMA_SEPARATOR = '_';

    public static String getSchemaFromFileName(String fileName) throws InvalidSchemaNameException, InvalidFileNameException {
        int start = -1, end = -1;
        final char[] chars = fileName.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            final char currentChar = chars[i];
            final boolean isUpperCase = Character.isUpperCase(currentChar);
            if (isUpperCase) {
                if (start < 0) {
                    start = i;
                }
            } else if (start >= 0 && currentChar == DEFAULT_SCHEMA_SEPARATOR && !isLetterInUpperCase(chars[i + 1])) {
                end = i;
                break;
            }
        }
        if (start >= 0 && end >= 0) {
            final String schemaName = fileName.substring(start, end);
            if (schemaName.toUpperCase().equals(schemaName)) {
                return schemaName;
            }
            throw new InvalidSchemaNameException(schemaName);
        }
        throw new InvalidFileNameException(fileName);
    }

    private static boolean isLetterInUpperCase(char symbol) {
        return Character.isLetter(symbol) && Character.isUpperCase(symbol);
    }
}
