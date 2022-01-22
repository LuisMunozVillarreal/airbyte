package io.airbyte.integrations.source.e2e_test;

import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaPrimitive;

public class TestingSourceConstants {

  public static final AirbyteCatalog DEFAULT_CATALOG = CatalogHelpers.createAirbyteCatalog(
      "data",
      Field.of("column1", JsonSchemaPrimitive.STRING));

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
