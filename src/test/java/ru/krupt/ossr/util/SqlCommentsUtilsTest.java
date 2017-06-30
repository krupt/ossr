package ru.krupt.ossr.util;

import org.junit.Assert;
import org.junit.Test;

public class SqlCommentsUtilsTest {

    @Test
    public void testWithoutComments() {
        Assert.assertEquals("  INSERT INTO dsads VALUES(1)", SqlCommentsUtils.skipSqlComments("\n\n"
                + "  INSERT INTO dsads\n"
                + "VALUES(1)"));
    }

    @Test
    public void testSkipSqlCommentsWithLineComment() {
        Assert.assertEquals("  INSERT INTO dsads   VALUES(1)", SqlCommentsUtils.skipSqlComments("\n-- Some line comment\n"
                + "  INSERT INTO dsads\n"
                + "  VALUES(1)"));
    }

    @Test
    public void testSkipSqlCommentsWithMultiLineComment() {
        Assert.assertEquals("delete t_dic_order_type t  where t.type_id=13", SqlCommentsUtils.skipSqlComments("/*select * from t_dic_order_type\n"
                + "--  13  Замена реквизитов физического лица\n"
                + "   10111  Изменение аттрибутов клиента\n"
                + "10110  Изменение аттрибутов клиента для основного устройства\n"
                + "\n"
                + "select order_type,count(*) from t_orders o where o.order_type in (13,10111,10110)\n"
                + "group by order_type*/\n"
                + "\n"
                + "delete t_dic_order_type t  where t.type_id=13\n"));
    }

    @Test
    public void testSkipSqlCommentsWithMultiLineCommentInOneLine() {
        Assert.assertEquals("delete t_dic_order_type t  where t.type_id=13",
                SqlCommentsUtils.skipSqlComments("delete t_dic_order_type t /* Таблица типов заказов*/ where t.type_id=13")
        );
    }

    @Test
    public void testSkipSqlCommentsWithMultiLineCommentInMiddleOnLines() {
        Assert.assertEquals("delete t_dic_order_type t   where t.type_id=13",
                SqlCommentsUtils.skipSqlComments("delete t_dic_order_type t /* Таблица\n"
                        + "типов заказов*/ where t.type_id=13")
        );
    }

    @Test
    public void testSkipSqlCommentsWithLineCommentAndDummyMultiLineComment() {
        Assert.assertEquals("delete t_dic_order_type t   where t.type_id=13",
                SqlCommentsUtils.skipSqlComments("-- Line comment /* Dummy comment \ndelete t_dic_order_type t /* Таблица\n"
                        + "типов заказов*/ where t.type_id=13")
        );
    }

    @Test
    public void testSkipSqlCommentsInFirstLines() {
        Assert.assertEquals("CREATE OR REPLACE PACKAGE BODY GRAN_Exchange AS   cPackageName          CONSTANT VARCHAR2(20) := 'GRAN_Exchange';"
                        + "   cBranch               CONSTANT NUMBER := 1;   cBranchPartGRAN       CONSTANT NUMBER := 1;"
                        + "   cExchModuleBankier    CONSTANT NUMBER := 6;   cEntCodeAdjustment    CONSTANT NUMBER := 32;",
                SqlCommentsUtils.skipSqlComments("/**\n"
                        + " * Package for export transaction to ABS\n"
                        + " */\n"
                        + "\n"
                        + "CREATE OR REPLACE PACKAGE BODY GRAN_Exchange\n"
                        + "AS\n"
                        + "/********************************************************************************************************************************\\\n"
                        + "  *** Created on 16.11.2012 by A.Kovalev\n"
                        + "  *** Start date of using on production system is 19.11.2012\n"
                        + "List of changes:\n"
                        + "----------------------------------------------------------------------------------------------------------------------------------\n"
                        + "Date       Author          Reason            Ident     Description\n"
                        + "---------- --------------- ----------------- --------- "
                        + "---------------------------------------------------------------------------\n"
                        + "2012.11.19 A.Kovalev       Bankier defect    EXCH-1    Unload tran with debitentcode = 32 and branchpart = 1 into second "
                        + "file\n"
                        + "2012.11.20 A.Kovalev       Execution control SECUR-1   Append data to log table in BankierDlg\n"
                        + "2012.11.23 A.Kovalev       Economs stupid    EXCH-2    Unload tran with debitentcode = 194 into others file group by "
                        + "branchpart\n"
                        + "2012.11.28 A.Kovalev       Bug               EXCH-3    Changed GetRetailerContract\n"
                        + "2012.12.05 A.Kovalev       Logic change      EXCH-4    USD and EUR accounts allowed for P2P\n"
                        + "2012.12.10 A.Kovalev       Compass+ bug      EXCH-5    Replace invalid accounts in issuing and acquiring\n"
                        + "2012.12.12 A.Kovalev       New scheme of POS EXCH-6    Added GetFeeRateByEntry\n"
                        + "2012.12.12 A.Kovalev       New scheme of POS EXCH-7    Change GetDetail\n"
                        + "2013.02.08 A.Kovalev       New scheme of BOI EXCH-8    Don't unload B14 and B15 in BOI-terminals\n"
                        + "2013.03.25 A.Kovalev       Ret. term. comis. EXCH-9    Detail for retailers terminals commission\n"
                        + "2013.05.07 A.Kovalev       New version TWCMS EXCH-10   Added InitBranch and GetBranch\n"
                        + "2013.08.12 A.Kovalev       Delete symbols    EXCH-11   Delete CHR(0) from GetDetail\n"
                        + "2013.10.17 A.Kovalev       Replace accounts  EXCH-12   Replace 40802*, 40702* accounts on 474*(from client user "
                        + "properties) in retailer's payments\n"
                        + "\\********************************************************************************************************************************/\n"
                        + "\n"
                        + "  cPackageName          CONSTANT VARCHAR2(20) := 'GRAN_Exchange';\n"
                        + "  cBranch               CONSTANT NUMBER := 1;\n"
                        + "  cBranchPartGRAN       CONSTANT NUMBER := 1;\n"
                        + "  cExchModuleBankier    CONSTANT NUMBER := 6;\n"
                        + "  cEntCodeAdjustment    CONSTANT NUMBER := 32;\n"
                        + "  cEntCodeRetailPayment CONSTANT NUMBER := 194;\n"
                        + "  cPrefixAdjustGRAN     CONSTANT VARCHAR2(5) := 'B1t';\n"
                        + "  cPrefixRetailPayment  CONSTANT VARCHAR2(5) := 'B~p';", 33)
        );
    }

    @Test
    public void testSkipSqlCommentsInOnlyLineComments() {
        Assert.assertEquals("",
                SqlCommentsUtils.skipSqlComments("-- Comment 1\n-- Comment 2\n-- Comment 3\n-- Comment 4\n-- Comment 5\n"
                        + "-- Comment 6\n-- Comment 7\n-- Comment 8\n-- Comment 9\n-- Comment 10\n", 10)
        );
    }

    @Test
    public void testSkipSqlCommentsInOnlyComments() {
        Assert.assertEquals("",
                SqlCommentsUtils.skipSqlComments("/* Comment line 1\n Comment line 2\n Comment line 3\n Comment line 4\n Comment line 5\n"
                        + " Comment line 6\n Comment line 7\n Comment line 8\n Comment line 9\n Comment line 10\n Comment line 11\n", 10)
        );
    }
}
