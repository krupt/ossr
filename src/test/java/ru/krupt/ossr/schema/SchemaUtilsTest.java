package ru.krupt.ossr.schema;

import org.junit.Assert;
import org.junit.Test;

public class SchemaUtilsTest {

    @Test
    public void testSimpleName() {
        Assert.assertEquals("KRUPT", SchemaUtils.getSchemaFromFileName("01_KRUPT_create_tables.sql"));
    }

    @Test
    public void testWithoutNumberPrefix() {
        Assert.assertEquals("KRUPT", SchemaUtils.getSchemaFromFileName("KRUPT_create_tables.sql"));
    }

    @Test
    public void testStartsWithDefaultSeparator() {
        Assert.assertEquals("KRUPT", SchemaUtils.getSchemaFromFileName("_KRUPT_create_tables.sql"));
    }

    @Test
    public void testSchemaWithDefaultSeparator() {
        Assert.assertEquals("KRUPT_ADS", SchemaUtils.getSchemaFromFileName("_KRUPT_ADS_create_tables.sql"));
    }

    /**
     * Not supported, because can't valid correct name.
     */
    @Test
    public void testSchemaWithDefaultSeparatorAndNumber() {
        Assert.assertEquals("KRUPT", SchemaUtils.getSchemaFromFileName("_KRUPT_1_create_tables.sql"));
    }

    @Test
    public void testSchemaWithCustomSeparator() {
        Assert.assertEquals("KRUPT#DSA", SchemaUtils.getSchemaFromFileName("_KRUPT#DSA_create_tables.sql"));
    }

    @Test
    public void testSchemaWithCustomSeparatorAndNumber() {
        Assert.assertEquals("KRUPT#1", SchemaUtils.getSchemaFromFileName("_KRUPT#1_create_tables.sql"));
    }

    @Test(expected = InvalidSchemaNameException.class)
    public void testSchemaWithCamelCase() {
        Assert.assertEquals("KRUPT", SchemaUtils.getSchemaFromFileName("_KRUPTisJKE_create_tables.sql"));
    }

    @Test(expected = InvalidSchemaNameException.class)
    public void testSchemaWithCamelCaseOneSymbol() {
        Assert.assertEquals("KRUPT", SchemaUtils.getSchemaFromFileName("_KRUPTiJKE_create_tables.sql"));
    }

    @Test(expected = InvalidSchemaNameException.class)
    public void testSchemaWithCamelCaseMoreSymbols() {
        Assert.assertEquals("KRUPT", SchemaUtils.getSchemaFromFileName("_KRUPT_Create_tables.sql"));
    }
}
