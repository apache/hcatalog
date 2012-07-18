package org.apache.hcatalog.pig;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.UDFContext;
import org.junit.Test;

public class TestPigHCatUtil {

  @Test
  public void testGetBagSubSchema() throws Exception {

    // Define the expected schema.
    ResourceFieldSchema[] bagSubFieldSchemas = new ResourceFieldSchema[1];
    bagSubFieldSchemas[0] = new ResourceFieldSchema().setName("innertuple")
        .setDescription("The tuple in the bag").setType(DataType.TUPLE);

    ResourceFieldSchema[] innerTupleFieldSchemas = new ResourceFieldSchema[1];
    innerTupleFieldSchemas[0] =
        new ResourceFieldSchema().setName("innerfield").setType(DataType.CHARARRAY);

    bagSubFieldSchemas[0].setSchema(new ResourceSchema().setFields(innerTupleFieldSchemas));
    ResourceSchema expected = new ResourceSchema().setFields(bagSubFieldSchemas);

    // Get the actual converted schema.
    HCatSchema hCatSchema = new HCatSchema(Lists.newArrayList(
        new HCatFieldSchema("innerLlama", HCatFieldSchema.Type.STRING, null)));
    HCatFieldSchema hCatFieldSchema =
        new HCatFieldSchema("llama", HCatFieldSchema.Type.ARRAY, hCatSchema, null);
    ResourceSchema actual = PigHCatUtil.getBagSubSchema(hCatFieldSchema);

    Assert.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testGetBagSubSchemaConfigured() throws Exception {

    // NOTE: pig-0.8 sets client system properties by actually getting the client
    // system properties. Starting in pig-0.9 you must pass the properties in.
    // When updating our pig dependency this will need updated.
    System.setProperty(HCatConstants.HCAT_PIG_INNER_TUPLE_NAME, "t");
    System.setProperty(HCatConstants.HCAT_PIG_INNER_FIELD_NAME, "FIELDNAME_tuple");
    UDFContext.getUDFContext().setClientSystemProps();

    // Define the expected schema.
    ResourceFieldSchema[] bagSubFieldSchemas = new ResourceFieldSchema[1];
    bagSubFieldSchemas[0] = new ResourceFieldSchema().setName("t")
        .setDescription("The tuple in the bag").setType(DataType.TUPLE);

    ResourceFieldSchema[] innerTupleFieldSchemas = new ResourceFieldSchema[1];
    innerTupleFieldSchemas[0] =
        new ResourceFieldSchema().setName("llama_tuple").setType(DataType.CHARARRAY);

    bagSubFieldSchemas[0].setSchema(new ResourceSchema().setFields(innerTupleFieldSchemas));
    ResourceSchema expected = new ResourceSchema().setFields(bagSubFieldSchemas);

    // Get the actual converted schema.
    HCatSchema actualHCatSchema = new HCatSchema(Lists.newArrayList(
        new HCatFieldSchema("innerLlama", HCatFieldSchema.Type.STRING, null)));
    HCatFieldSchema actualHCatFieldSchema =
        new HCatFieldSchema("llama", HCatFieldSchema.Type.ARRAY, actualHCatSchema, null);
    ResourceSchema actual = PigHCatUtil.getBagSubSchema(actualHCatFieldSchema);

    Assert.assertEquals(expected.toString(), actual.toString());
  }
}
