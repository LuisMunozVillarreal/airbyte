package io.airbyte.integrations.source.e2e_test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import javax.annotation.CheckForNull;

public class ContinuousFeedSource extends BaseConnector implements Source {

  @Override
  public AirbyteConnectionStatus check(final JsonNode jsonConfig) {
    try {
      final ContinuousFeedConfig sourceConfig = new ContinuousFeedConfig(jsonConfig);
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED).withMessage("Source config: " + sourceConfig);
    } catch (final Exception e) {
      return new AirbyteConnectionStatus().withStatus(Status.FAILED).withMessage(e.getMessage());
    }
  }

  @Override
  public AirbyteCatalog discover(final JsonNode jsonConfig) throws Exception {
    final ContinuousFeedConfig sourceConfig = new ContinuousFeedConfig(jsonConfig);
    return sourceConfig.getMockCatalog();
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode jsonConfig, final ConfiguredAirbyteCatalog catalog, final JsonNode state) throws Exception {
    final ContinuousFeedConfig feedConfig = new ContinuousFeedConfig(jsonConfig);
    final Predicate<Long> hasMoreMessages = recordNumber -> recordNumber < feedConfig.getMaxMessages();

    final List<Iterator<AirbyteMessage>> iterators = new LinkedList<>();
    for (final ConfiguredAirbyteStream stream : catalog.getStreams()) {
      final JsonNode streamSchema = stream.getStream().getJsonSchema();
      final AtomicLong emittedMessages = new AtomicLong(0);
      final Iterator<AirbyteMessage> streamIterator = new AbstractIterator<>() {
        @CheckForNull
        @Override
        protected AirbyteMessage computeNext() {
          if (emittedMessages.get() >= feedConfig.getMaxMessages()) {
            return endOfData();
          }

          if (feedConfig.getMessageIntervalMs().isPresent() && emittedMessages.get() != 0) {
            try {
              Thread.sleep(feedConfig.getMessageIntervalMs().get());
            } catch (final InterruptedException e) {
              throw new RuntimeException(e);
            }
          }

          emittedMessages.incrementAndGet();
          return new AirbyteMessage()
              .withType(Type.RECORD)
              .withRecord(new AirbyteRecordMessage()
                  .withStream(stream.getStream().getName())
                  .withData(null));
        }
      };
      iterators.add(streamIterator);
    }

    return AutoCloseableIterators.fromIterator(Iterators.concat(iterators.iterator()));
  }

}
