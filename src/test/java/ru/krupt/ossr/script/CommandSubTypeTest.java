package ru.krupt.ossr.script;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CommandSubTypeTest {

    private final Map<String, CommandSubType> map = new HashMap<>();

    @Before
    public void init() {
        map.put("create synonym t_device_price_sale_vers for contract.t_device_price_sale_vers", CommandSubType.CREATE);
        map.put("COMMENT ON COLUMN t_dic_mvno_region.without_search_phys\n" +
                        "  IS 'Возможно ли подключение MVNO для физических лиц без поиска фиксированных услуг (0 - запрещено, 1 - разрешено)'",
                CommandSubType.COMMENT
        );
        map.put("ALTER TABLE t_dic_mvno_region ADD without_search_phys NUMBER(1) DEFAULT 0 NOT NULL", CommandSubType.ALTER);
        map.put("UPDATE users SET password = '123'", CommandSubType.UPDATE);
        map.put("DELETE FROM roles", CommandSubType.DELETE);
        map.put("INSERT INTO disabled(name, value) VALUES('dsads', 1)", CommandSubType.INSERT);
        map.put("BEGIN NULL; END;", CommandSubType.UNKNOWN);
        map.put("DECLARE v_option NUMBER; BEGIN NULL; END;", CommandSubType.UNKNOWN);
    }

    @Test
    public void test() {
        for (Map.Entry<String, CommandSubType> entry : map.entrySet()) {
            Assert.assertEquals(entry.getKey(), entry.getValue(), CommandSubType.of(entry.getKey().toLowerCase()));
        }
    }
}
