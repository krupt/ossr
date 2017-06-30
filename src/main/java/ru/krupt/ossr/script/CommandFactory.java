package ru.krupt.ossr.script;

import ru.krupt.ossr.util.SqlCommentsUtils;
import ru.krupt.ossr.util.StringUtils;

public class CommandFactory {

    static Command instantiateCommand(String commandText, CommandType type) {
        final String compressedCommandText = SqlCommentsUtils.skipSqlComments(commandText, 50).toLowerCase();
        final CommandSubType subType = CommandSubType.of(compressedCommandText);
        final ObjectType objectType = ObjectType.of(compressedCommandText);
        return new Command(type, subType, objectType, commandText);
    }

    public static String getCommandResultDescription(Command command, int affectedRows) {
        if (command.getType() == CommandTypes.ANONYMOUS_PLSQL_BLOCK) {
            return "PL/SQL procedure successfully completed";
        } else {
            final CommandSubType subType = command.getSubType();
            if (subType == CommandSubType.UPDATE || subType == CommandSubType.INSERT || subType == CommandSubType.DELETE) {
                String operationName = affectedRows + " row";
                if (affectedRows != 1) {
                    operationName += "s";
                }
                operationName += " " + subType.name().toLowerCase();
                if (subType == CommandSubType.INSERT) {
                    operationName += "ed";
                } else {
                    operationName += "d";
                }
                return operationName;
            } else if (subType == CommandSubType.COMMENT) {
                return "Comment added";
            } else if (subType == CommandSubType.GRANT || subType == CommandSubType.REVOKE) {
                return StringUtils.capitalize(subType.name()) + " succeeded";
            } else if (subType == CommandSubType.CREATE || subType == CommandSubType.DROP || subType == CommandSubType.ALTER) {
                final ObjectType objectType = command.getObjectType();
                return StringUtils.capitalize(objectType.name()).replace('_', ' ')
                        + (subType == CommandSubType.CREATE ? " created" : subType == CommandSubType.ALTER ? " altered" : " dropped");
            }
        }
        return "Query executed";
    }
}
