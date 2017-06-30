package ru.krupt.ossr.script;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CommandTypeFactoryTest {

    private final Map<String, CommandTypes> map = new HashMap<>();

    @Before
    public void init() {
        map.put("create synonym t_device_price_sale_vers for contract.t_device_price_sale_vers", CommandTypes.DEFAULT);
        map.put("COMMENT ON COLUMN t_dic_mvno_region.without_search_phys\n" +
                        "  IS 'Возможно ли подключение MVNO для физических лиц без поиска фиксированных услуг (0 - запрещено, 1 - разрешено)'",
                CommandTypes.DEFAULT
        );
        map.put("ALTER TABLE t_dic_mvno_region ADD without_search_phys NUMBER(1) DEFAULT 0 NOT NULL", CommandTypes.DEFAULT);
        map.put("UPDATE users SET password = '123'", CommandTypes.DEFAULT);
        map.put("DELETE FROM roles", CommandTypes.DEFAULT);
        map.put("INSERT INTO disabled(name, value) VALUES('dsads', 1)", CommandTypes.DEFAULT);
        map.put("BEGIN NULL; END;", CommandTypes.ANONYMOUS_PLSQL_BLOCK);
        map.put("DECLARE v_option NUMBER; BEGIN NULL; END;", CommandTypes.ANONYMOUS_PLSQL_BLOCK);
        map.put("CREATE TABLE dsa(v_option NUMBER);", CommandTypes.DEFAULT);
        map.put("CREATE PACKAGE BODY asdsa AS END;", CommandTypes.PLSQL_OBJECT);
        map.put("CREATE OR REPLACE PACKAGE asdsa AS END;", CommandTypes.PLSQL_OBJECT);
        map.put("CREATE OR REPLACE PROCEDURE asdsa AS BEGIN NULL; END;", CommandTypes.PLSQL_OBJECT);
        map.put("CREATE OR REPLACE FUNCTION asdsa RETURN VARCHAR2 AS BEGIN NULL; END;", CommandTypes.PLSQL_OBJECT);
        map.put("CREATE OR REPLACE trigger asdsa before insert on t_table BEGIN NULL; END;", CommandTypes.PLSQL_OBJECT);
        map.put("CREATE OR REPLACE type asdsa (dsa NUMBER)", CommandTypes.PLSQL_OBJECT);
        map.put("CREATE OR REPLACE type body asdsa (dsa NUMBER)", CommandTypes.PLSQL_OBJECT);
    }

    @Test
    public void test() {
        for (Map.Entry<String, CommandTypes> entry : map.entrySet()) {
            Assert.assertEquals(entry.getKey(), entry.getValue(), CommandTypeFactory.getCommand(entry.getKey().toLowerCase()));
        }
    }
}
