package io.airbyte.integrations.source.e2e_test;

import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaPrimitive;

public class TestingSourceConstants {

  public static final String DEFAULT_STREAM = "data";
  public static final String DEFAULT_COLUMN = "column1";
  public static final AirbyteCatalog DEFAULT_CATALOG = CatalogHelpers.createAirbyteCatalog(
      DEFAULT_STREAM,
      Field.of(DEFAULT_COLUMN, JsonSchemaPrimitive.STRING));

  public enum MockBehaviorType {
    CONTINUOUS_FEED,
    EXCEPTION_AFTER_N
  }

  public enum MockCatalogType {
    DEFAULT,
    SINGLE_STREAM,
    MULTI_STREAM
  }

}