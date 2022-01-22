package io.airbyte.integrations.source.e2e_test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.commons.jackson.MoreMappers;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.commons.util.MoreIterators;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.validation.json.JsonValidationException;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestingSourceConfigTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestingSourceConfigTest.class);

  private static final ObjectMapper MAPPER = MoreMappers.initMapper();
  private static final Random RANDOM = new Random();

  @Test
  public void testParseSeed() {
    final long expectedSeed = RANDOM.nextLong();
    final long actualSeed = TestingSourceConfig.parseSeed(Jsons.deserialize(String.format("{ \"seed\": %d }", expectedSeed)));
    assertEquals(expectedSeed, actualSeed);
  }

  @ParameterizedTest
  @ArgumentsSource(ParseMockCatalogTestCaseProvider.class)
  public void testParseMockCatalog(final String testCaseName,
                                   final JsonNode mockConfig,
                                   final boolean invalidSchema,
                                   final AirbyteCatalog expectedCatalog) throws Exception {
    if (invalidSchema) {
      try {
        TestingSourceConfig.parseMockCatalog(mockConfig);
        fail();
      } catch (final JsonValidationException e) {
        // expected
        LOGGER.info("Json validation message: {}", e.getMessage());
      }
    } else {
      final AirbyteCatalog actualCatalog = TestingSourceConfig.parseMockCatalog(mockConfig);
      assertEquals(expectedCatalog.getStreams(), actualCatalog.getStreams());
    }
  }

  public static class ParseMockCatalogTestCaseProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
      final JsonNode testCases =
          Jsons.deserialize(MoreResources.readResource("testing_source_config/parse_mock_catalog_test_cases.json"));
      return MoreIterators.toList(testCases.elements()).stream().map(testCase -> {
        final JsonNode mockConfig = MAPPER.createObjectNode().set("mock_catalog", testCase.get("mockCatalog"));
        final boolean invalidSchema = testCase.has("invalidSchema") && testCase.get("invalidSchema").asBoolean();
        final AirbyteCatalog expectedCatalog = invalidSchema ? null : Jsons.object(testCase.get("expectedCatalog"), AirbyteCatalog.class);
        return Arguments.of(
            testCase.get("testCase").asText(),
            mockConfig,
            invalidSchema,
            expectedCatalog);
      });
    }

  }

}
