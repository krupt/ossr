package ru.krupt.ossr.script;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ObjectTypeTest {

    private final Map<String, ObjectType> map = new HashMap<>();

    @Before
    public void init() {
        map.put("create synonym t_device_price_sale_vers for contract.t_device_price_sale_vers", ObjectType.SYNONYM);
        map.put("COMMENT ON COLUMN t_dic_mvno_region.without_search_phys\n" +
                        "  IS 'Возможно ли подключение MVNO для физических лиц без поиска фиксированных услуг (0 - запрещено, 1 - разрешено)'",
                ObjectType.UNKNOWN
        );
        map.put("ALTER TABLE t_dic_mvno_region ADD without_search_phys NUMBER(1) DEFAULT 0 NOT NULL", ObjectType.TABLE);
        map.put("UPDATE users SET password = '123'", ObjectType.UNKNOWN);
        map.put("DELETE FROM roles", ObjectType.UNKNOWN);
        map.put("INSERT INTO disabled(name, value) VALUES('dsads', 1)", ObjectType.UNKNOWN);
        map.put("BEGIN NULL; END;", ObjectType.UNKNOWN);
        map.put("DECLARE v_option NUMBER; BEGIN NULL; END;", ObjectType.UNKNOWN);
        map.put("CREATE TABLE dsa(v_option NUMBER);", ObjectType.TABLE);
        map.put("CREATE PACKAGE BODY asdsa AS END;", ObjectType.PACKAGE_BODY);
        map.put("CREATE OR REPLACE PACKAGE asdsa AS END;", ObjectType.PACKAGE);
        map.put("CREATE OR REPLACE PROCEDURE asdsa AS BEGIN NULL; END;", ObjectType.PROCEDURE);
        map.put("CREATE OR REPLACE FUNCTION asdsa RETURN VARCHAR2 AS BEGIN NULL; END;", ObjectType.FUNCTION);
        map.put("CREATE OR REPLACE trigger asdsa before insert on t_table BEGIN NULL; END;", ObjectType.TRIGGER);
        map.put("CREATE OR REPLACE type asdsa (dsa NUMBER)", ObjectType.TYPE);
        map.put("CREATE OR REPLACE type body asdsa (dsa NUMBER)", ObjectType.TYPE_BODY);
        map.put("CREATE OR REPLACE PACKAGE BODY ASR_IMPORT_TAR is type rec_tar is record(\n" +
                "    tar_id             number); END;", ObjectType.PACKAGE_BODY);
    }

    @Test
    public void test() {
        for (Map.Entry<String, ObjectType> entry : map.entrySet()) {
            Assert.assertEquals(entry.getKey(), entry.getValue(), ObjectType.of(entry.getKey()));
        }
    }
}
