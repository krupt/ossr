package ru.krupt.ossr.script;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CommandFactoryTest {

    private final Map<Command, String> map = new HashMap<>();

    @Before
    public void init() {
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.UPDATE, ObjectType.UNKNOWN, ""), "33 rows updated");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.INSERT, ObjectType.UNKNOWN, ""), "33 rows inserted");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.DELETE, ObjectType.UNKNOWN, ""), "33 rows deleted");
        map.put(new Command(CommandTypes.ANONYMOUS_PLSQL_BLOCK, CommandSubType.UNKNOWN, ObjectType.UNKNOWN, ""),
                "PL/SQL procedure successfully completed");

        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.CREATE, ObjectType.TABLE, ""), "Table created");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.ALTER, ObjectType.TABLE, ""), "Table altered");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.COMMENT, ObjectType.TABLE, ""), "Comment added");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.COMMENT, ObjectType.UNKNOWN, ""), "Comment added");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.GRANT, ObjectType.UNKNOWN, ""), "Grant succeeded");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.REVOKE, ObjectType.UNKNOWN, ""), "Revoke succeeded");

        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.CREATE, ObjectType.SEQUENCE, ""), "Sequence created");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.DROP, ObjectType.SEQUENCE, ""), "Sequence dropped");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.CREATE, ObjectType.SYNONYM, ""), "Synonym created");
        map.put(new Command(CommandTypes.DEFAULT, CommandSubType.CREATE, ObjectType.INDEX, ""), "Index created");

        map.put(new Command(CommandTypes.PLSQL_OBJECT, CommandSubType.CREATE, ObjectType.PACKAGE_BODY, ""), "Package body created");
        map.put(new Command(CommandTypes.PLSQL_OBJECT, CommandSubType.CREATE, ObjectType.PACKAGE, ""), "Package created");
        map.put(new Command(CommandTypes.PLSQL_OBJECT, CommandSubType.CREATE, ObjectType.TYPE_BODY, ""), "Type body created");
        map.put(new Command(CommandTypes.PLSQL_OBJECT, CommandSubType.CREATE, ObjectType.TYPE, ""), "Type created");
        map.put(new Command(CommandTypes.PLSQL_OBJECT, CommandSubType.CREATE, ObjectType.PROCEDURE, ""), "Procedure created");
        map.put(new Command(CommandTypes.PLSQL_OBJECT, CommandSubType.CREATE, ObjectType.FUNCTION, ""), "Function created");
        map.put(new Command(CommandTypes.PLSQL_OBJECT, CommandSubType.CREATE, ObjectType.TRIGGER, ""), "Trigger created");

        // CREATE, ALTER, COMMENT, GRANT, REVOKE
        // TABLE, SEQUENCE, SYNONYM, INDEX, PACKAGE_BODY, PACKAGE, TYPE_BODY, TYPE, PROCEDURE, FUNCTION, TRIGGER
    }

    @Test
    public void test() {
        for (Map.Entry<Command, String> entry : map.entrySet()) {
            Assert.assertEquals(entry.getKey().toString(), entry.getValue(), CommandFactory.getCommandResultDescription(entry.getKey(), 33));
        }
        Assert.assertEquals("1 row updated",
                CommandFactory.getCommandResultDescription(new Command(CommandTypes.DEFAULT, CommandSubType.UPDATE, ObjectType.UNKNOWN, ""), 1)
        );
    }
}
