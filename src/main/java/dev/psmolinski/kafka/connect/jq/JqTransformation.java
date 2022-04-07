package dev.psmolinski.kafka.connect.jq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The transformation applies `jq` expression either to key or value of the message
 * and puts the result in the same place as the source.
 * @param <R>
 */
public abstract class JqTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

  private JsonQuery query;

  public abstract R apply(R record);

  @Override
  public ConfigDef config() {
    return JqTransformationConfig.CONFIG_DEF;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

    JqTransformationConfig config = new JqTransformationConfig(configs);

    try {
      this.query = JsonQuery.compile(config.getQuery());
    } catch (Exception e) {
      throw new ConnectException("Cannot compile jq query: " + config.getQuery(), e);
    }

  }

  public Object transform(Schema schema, Object object) {
    if (query == null) {
      throw new IllegalStateException("query null, transformation not configured");
    }
    if (object == null) {
      return null;
    }
    if (object instanceof String) {
      return transform((String)object);
    }
    throw new ConnectException("Unsupported input type: "+object.getClass());
  }

  public String transform(String json) {

    final ObjectMapper om = new ObjectMapper();
    final JsonNode parsed;
    try {
      parsed = om.readTree(json);
    } catch (Exception e) {
      throw new ConnectException("Failed executing jq query", e);
    }

    Scope rootScope = Scope.newEmptyScope();
    rootScope.loadFunctions(Scope.class.getClassLoader());

    final List<JsonNode> result;
    try {
      result = query.apply(rootScope, parsed);
    } catch (Exception e) {
      throw new ConnectException("Failed executing jq query", e);
    }

    if (result == null) {
      return "";
    }

    return result.stream()
            .map(node->{
              try {
                return om.writeValueAsString(node);
              } catch (Exception e) {
                throw new ConnectException(e);
              }
            })
            .collect(Collectors.joining());

  }

  public static class Key<R extends ConnectRecord<R>> extends JqTransformation<R> {
    public R apply(R record) {
      return record.newRecord(
              record.topic(),
              record.kafkaPartition(),
              record.keySchema(),
              transform(record.keySchema(), record.key()),
              record.valueSchema(),
              record.value(),
              record.timestamp()
      );
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends JqTransformation<R> {
    public R apply(R record) {
      return record.newRecord(
              record.topic(),
              record.kafkaPartition(),
              record.keySchema(),
              record.key(),
              record.valueSchema(),
              transform(record.valueSchema(), record.value()),
              record.timestamp()
      );
    }
  }

}
