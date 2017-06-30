package ru.krupt.ossr.script;

import lombok.experimental.Delegate;
import ru.krupt.ossr.command.AnonymousPlSqlBlock;
import ru.krupt.ossr.command.DefaultSqlCommand;
import ru.krupt.ossr.command.PlSqlObject;

public enum CommandTypes implements CommandType {

    DEFAULT(new DefaultSqlCommand()),
    ANONYMOUS_PLSQL_BLOCK(new AnonymousPlSqlBlock()),
    PLSQL_OBJECT(new PlSqlObject());

    @Delegate
    private final CommandType commandType;

    CommandTypes(CommandType commandType) {
        this.commandType = commandType;
    }
}
