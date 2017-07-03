package ru.krupt.ossr.script;

import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ScriptParserTest {

    @Test
    public void testSqlAndAnonymousPlSqlBlockFile() throws IOException {
        try (final InputStream resource = getClass().getResourceAsStream("/testPatch/00_EISSD_ab_service.sql")) {
            final List<Command> commands = (List<Command>) ScriptParser.parseFile(resource);
            Assert.assertEquals(14, commands.size());
            final Command command = commands.get(0);
            Assert.assertEquals(CommandTypes.DEFAULT, command.getType());
            Assert.assertEquals(CommandSubType.COMMENT, command.getSubType());
            Assert.assertTrue(command.getText().contains("COMMENT ON COLUMN t_dic_ab_service.connection_type"));
            final Command command1 = commands.get(1);
            Assert.assertEquals(CommandTypes.DEFAULT, command1.getType());
            Assert.assertEquals(CommandSubType.INSERT, command1.getSubType());
            Assert.assertTrue(command1.getText().endsWith("(258, 'Перемещение услуг между ЛС / Переоформление услуг', 'Перемещение услуг между ЛС / "
                    + "Переоформление услуг', 1, 'P')"));
            final Command command2 = commands.get(2);
            Assert.assertEquals(CommandTypes.DEFAULT, command2.getType());
            Assert.assertEquals(CommandSubType.INSERT, command2.getSubType());
            Assert.assertTrue(command2.getText().endsWith("(259, 'Перемещение услуг между ЛС / Переоформление услуг', 'Перемещение услуг между ЛС / "
                    + "Переоформление услуг', 2, 'P')"));
            final Command command3 = commands.get(3);
            Assert.assertEquals(CommandTypes.DEFAULT, command3.getType());
            Assert.assertEquals(CommandSubType.INSERT, command3.getSubType());
            Assert.assertTrue(command3.getText().endsWith("(260, 'Перемещение услуг между ЛС / Переоформление услуг', 'Перемещение услуг между ЛС / "
                    + "Переоформление услуг', 5, 'P')"));
            final Command command4 = commands.get(4);
            Assert.assertEquals(CommandTypes.ANONYMOUS_PLSQL_BLOCK, command4.getType());
            Assert.assertEquals(CommandSubType.UNKNOWN, command4.getSubType());
            Assert.assertTrue(command4.getText().contains("('260_reissuance_5', 1, 'Переоформление услуг', 1, SYSDATE, 91951)"));
            final Command command11 = commands.get(11);
            Assert.assertEquals(CommandTypes.DEFAULT, command11.getType());
            Assert.assertEquals(CommandSubType.INSERT, command11.getSubType());
            Assert.assertEquals("INSERT INTO t_ab_option_avail /*\r\n"
                            + "                               * Adding options;\r\n"
                            + "                               */\r\n"
                            + "  (id_option, id_mrf, action, date_start, date_end, id_region)\r\n"
                            + "  SELECT id_option, mrf_id, CASE WHEN o.code LIKE '%$_transfer$_%' ESCAPE '$' THEN 1 ELSE 0 END, '17.04.2017', "
                            + "'31.12.2999', reg_id\r\n"
                            + "  FROM /* Option mapping */t_dic_ab_serv_opt so\r\n"
                            + "  JOIN t_dic_ab_options o ON o.id = so.id_option\r\n"
                            + "  CROSS JOIN t_dic_region\r\n"
                            + "  WHERE id_serv IN (258, 259, 260) /*\r\n"
                            + "                                    * Specific option ids;\r\n"
                            + "                                    */\r\n"
                            + "        AND mrf_id = 7",
                    command11.getText()
            );
            final Command command12 = commands.get(12);
            Assert.assertEquals(CommandTypes.DEFAULT, command12.getType());
            Assert.assertEquals(CommandSubType.INSERT, command12.getSubType());
            Assert.assertEquals("INSERT INTO t_ab_option_avail\r\n"
                            + "  (id_option, id_mrf, action, date_start, date_end, id_region)\r\n"
                            + "  SELECT id_option, mrf_id, CASE WHEN o.code LIKE '%$_transfer$_%' ESCAPE '$' THEN 1 ELSE 0 END, '17.04.2017', "
                            + "'31.12.2999', reg_id\r\n"
                            + "  FROM /* Option mapping; */t_dic_ab_serv_opt so\r\n"
                            + "  JOIN t_dic_ab_options o ON o.id = so.id_option\r\n"
                            + "  CROSS JOIN t_dic_region\r\n"
                            + "  WHERE id_serv IN (258, 259, 260) /*;\r\n"
                            + "       Only Ural;\r\n"
                            + "        ;*/ AND mrf_id = 7",
                    command12.getText()
            );
            final Command command13 = commands.get(13);
            Assert.assertEquals(CommandTypes.DEFAULT, command13.getType());
            Assert.assertEquals(CommandSubType.COMMIT, command13.getSubType());
        }
    }

    @Test
    public void testSqlFile() throws IOException {
        try (final InputStream resource = getClass().getResourceAsStream("/testPatch/07_EISSD_schema.sql")) {
            final List<Command> commands = (List<Command>) ScriptParser.parseFile(resource);
            Assert.assertEquals(5, commands.size());
            final Command command = commands.get(0);
            Assert.assertEquals(CommandTypes.DEFAULT, command.getType());
            Assert.assertEquals(CommandSubType.ALTER, command.getSubType());
            Assert.assertEquals("ALTER TABLE t_dic_mvno_region ADD without_search_phys NUMBER(1) DEFAULT 0 NOT NULL", command.getText());
            final Command command1 = commands.get(1);
            Assert.assertEquals(CommandTypes.DEFAULT, command1.getType());
            Assert.assertEquals(CommandSubType.ALTER, command1.getSubType());
            Assert.assertEquals("ALTER TABLE t_dic_mvno_region ADD without_search_jur NUMBER(1) DEFAULT 0 NOT NULL", command1.getText());
            final Command command2 = commands.get(2);
            Assert.assertEquals(CommandTypes.DEFAULT, command2.getType());
            Assert.assertEquals(CommandSubType.COMMENT, command2.getSubType());
            Assert.assertEquals("COMMENT ON COLUMN t_dic_mvno_region.without_search_phys\r\n"
                            + "  IS 'Возможно ли подключение MVNO для физических лиц без поиска фиксированных услуг (0 - запрещено, 1 - разрешено)'",
                    command2.getText()
            );
            final Command command3 = commands.get(3);
            Assert.assertEquals(CommandTypes.DEFAULT, command3.getType());
            Assert.assertEquals(CommandSubType.COMMENT, command3.getSubType());
            Assert.assertEquals("COMMENT ON COLUMN t_dic_mvno_region.without_search_jur\r\n"
                            + "  IS 'Возможно ли подключение MVNO для юридических лиц без поиска фиксированных услуг (0 - запрещено, 1 - разрешено)'",
                    command3.getText()
            );
            final Command command4 = commands.get(4);
            Assert.assertEquals(CommandTypes.PLSQL_OBJECT, command4.getType());
            Assert.assertEquals(CommandSubType.CREATE, command4.getSubType());
            Assert.assertEquals(ObjectType.PROCEDURE, command4.getObjectType());
            Assert.assertEquals("CREATE OR REPLACE PROCEDURE add_logging\r\n"
                            + "(\r\n"
                            + "  pi_action     IN t_logging.action%TYPE,\r\n"
                            + "  pi_time_begin IN t_logging.time_begin%TYPE,\r\n"
                            + "  pi_time_end   IN t_logging.time_end%TYPE,\r\n"
                            + "  pi_worker_id  IN t_logging.worker_id%TYPE,\r\n"
                            + "  pi_descript   IN t_logging.descript%TYPE,\r\n"
                            + "  po_err_num    OUT PLS_INTEGER,\r\n"
                            + "  po_err_msg    OUT VARCHAR2\r\n"
                            + ") IS\r\n"
                            + "  PRAGMA AUTONOMOUS_TRANSACTION;\r\n"
                            + "BEGIN\r\n"
                            + "  SAVEPOINT sp_add_logging;\r\n"
                            + "\r\n"
                            + "  INSERT INTO t_logging\r\n"
                            + "    (action, time_begin, time_end, worker_id, descript)\r\n"
                            + "  VALUES\r\n"
                            + "    (pi_action, pi_time_begin, pi_time_end, pi_worker_id, pi_descript);\r\n"
                            + "\r\n"
                            + "  COMMIT;\r\n"
                            + "EXCEPTION\r\n"
                            + "  WHEN OTHERS THEN\r\n"
                            + "    po_err_num := SQLCODE;\r\n"
                            + "    po_err_msg := SQLERRM || ' ' || dbms_utility.format_error_backtrace;\r\n"
                            + "    ROLLBACK TO sp_add_logging;\r\n"
                            + "END add_logging;",
                    command4.getText()
            );
        }
    }

    @Test
    public void testAllInDir() throws IOException, URISyntaxException {
        Files.list(Paths.get(getClass().getResource("/testPatch").toURI()))
                .filter(path -> !Files.isDirectory(path))
                /*.skip(8).limit(1)*/
                .forEach(path -> {
                    final String fileName = path.getFileName().toString();
                    System.out.println("fileName = " + fileName);
                    int expectedCommands = 0;
                    try {
                        final List<Command> commands = (List<Command>) ScriptParser.parseFile(new FileInputStream(path.toFile()));
                        switch (fileName) {
                            case "00_EISSD_ab_service.sql":
                                expectedCommands = 14;
                                break;
                            case "01_EISSD_create_tables.sql":
                                expectedCommands = 64;
                                break;
                            case "02_EISSD_fill_dictionaries.sql":
                                expectedCommands = 12;
                                break;
                            case "03_EISSD_create_rights.sql":
                                expectedCommands = 3;
                                break;
                            case "04_EISSD_insert_data_for_menu.sql":
                            case "07_EISSD_trigger.sql":
                            case "10_EISSD_mvno_pkg.pck":
                                expectedCommands = 2;
                                break;
                            case "05_EISSD_tables.sql":
                                expectedCommands = 8;
                                break;
                            case "07_EISSD_schema.sql":
                                expectedCommands = 5;
                                break;
                            case "11_EISSD_117394 изменение типа наряда.sql":
                                expectedCommands = 4;
                                break;
                            case "08_EISSD_ grant.sql":
                                expectedCommands = 6;
                                break;
                            case "06_EISSD_update_dogovors.sql":
                            case "08_EISSD_update_ps_region_mapping.sql":
                            case "09_EISSD_abonent_activ_list_type.tps":
                            case "10_EISSD_asr_import_tar.bdy":
                            case "11_EISSD_mvno_pkg.spc":
                            case "12_EISSD_mvno_pkg.bdy":
                            case "13_EISSD_orgs.spc":
                            case "14_EISSD_orgs.bdy":
                            case "15_EISSD_report.bdy":
                            case "16_EISSD_report_period.bdy":
                            case "17_EISSD_security_pkg.bdy":
                            case "18_EISSD_sim_imaging_tab.tps":
                            case "19_EISSD_sim_imaging_type.tps":
                            case "20_EISSD_tmc_ab.bdy":
                            case "21_EISSD_tmc_ott_stb.bdy":
                            case "22_EISSD_tmc_sim_imaging.spc":
                            case "23_EISSD_tmc_sim_imaging.bdy":
                            case "24_EISSD_user_pkg.spc":
                            case "25_EISSD_user_pkg.bdy":
                            case "26_EISSD_user_type.tps":
                            case "27_EISSD_users.spc":
                            case "28_EISSD_users.bdy":
                            case "29_IKESHPD_request_list.bdy":
                                expectedCommands = 1;
                        }
                        Assert.assertEquals(fileName, expectedCommands, commands.size());
                    } catch (FileNotFoundException e) {
                        throw new IllegalStateException(e);
                    }
                });
    }
}
