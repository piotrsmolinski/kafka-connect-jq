package dev.psmolinski.kafka.connect.jq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class JqTransformationTest {

  @Test
  void testIdentity() throws Exception {

    JqTransformation<SinkRecord> transformation = new JqTransformation.Value<>();
    transformation.configure(Collections.singletonMap("query", "."));

    SinkRecord record1 = new SinkRecord(
           "test",
           0,
           Schema.OPTIONAL_STRING_SCHEMA,
           null,
           Schema.OPTIONAL_STRING_SCHEMA,
           readResource("sample.json"),
           0L
    );

    SinkRecord record2 = transformation.apply(record1);

    ObjectMapper om = new ObjectMapper();
    Map<String, Object> payload = om.readValue((String)record2.value(), LinkedHashMap.class);

    Assertions.assertThat(payload).containsKey("header");
    Assertions.assertThat(payload).containsKey("event");
    Assertions.assertThat(payload).hasSize(2);

  }

  @Test
  void testAddField() throws Exception {

    JqTransformation<SinkRecord> transformation = new JqTransformation.Value<>();
    transformation.configure(Collections.singletonMap("query", ". + {\"test\": \"test\"}"));

    SinkRecord record1 = new SinkRecord(
            "test",
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.OPTIONAL_STRING_SCHEMA,
            readResource("sample.json"),
            0L
    );

    SinkRecord record2 = transformation.apply(record1);

    ObjectMapper om = new ObjectMapper();
    Map<String, Object> payload = om.readValue((String)record2.value(), LinkedHashMap.class);

    Assertions.assertThat(payload).containsKey("header");
    Assertions.assertThat(payload).containsKey("event");
    Assertions.assertThat(payload).containsKey("test");

    Assertions.assertThat(payload).hasSize(3);

  }

  @Test
  void testDropHeader() throws Exception {

    JqTransformation<SinkRecord> transformation = new JqTransformation.Value<>();
    transformation.configure(Collections.singletonMap("query", "del(.header)"));

    SinkRecord record1 = new SinkRecord(
            "test",
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.OPTIONAL_STRING_SCHEMA,
            readResource("sample.json"),
            0L
    );

    SinkRecord record2 = transformation.apply(record1);

    ObjectMapper om = new ObjectMapper();
    Map<String, Object> payload = om.readValue((String)record2.value(), LinkedHashMap.class);

    Assertions.assertThat(payload).containsKey("event");

    Assertions.assertThat(payload).hasSize(1);

  }

  @Test
  void testDropHeaderAbc() throws Exception {

    JqTransformation<SinkRecord> transformation = new JqTransformation.Value<>();
    transformation.configure(Collections.singletonMap("query", "del(.header.abc)"));

    SinkRecord record1 = new SinkRecord(
            "test",
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.OPTIONAL_STRING_SCHEMA,
            readResource("sample.json"),
            0L
    );

    SinkRecord record2 = transformation.apply(record1);

    ObjectMapper om = new ObjectMapper();
    Map<String, Object> payload = om.readValue((String)record2.value(), LinkedHashMap.class);

    Assertions.assertThat(payload).containsKey("header");
    Assertions.assertThat(payload).containsKey("event");
    Assertions.assertThat(payload).hasSize(2);

    Assertions.assertThat((Map<String,Object>)(payload.get("header"))).doesNotContainKey("abc");

  }

  @Test
  void testDropNestedField() throws Exception {

    JqTransformation<SinkRecord> transformation = new JqTransformation.Value<>();
    transformation.configure(Collections.singletonMap("query", ". + { event: (.event | (. + {payload: (.payload | fromjson | del(.field1) | tojson) } ))}"));

    SinkRecord record1 = new SinkRecord(
            "test",
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            null,
            Schema.OPTIONAL_STRING_SCHEMA,
            readResource("sample.json"),
            0L
    );

    SinkRecord record2 = transformation.apply(record1);

    ObjectMapper om = new ObjectMapper();
    Map<String, Object> payload = om.readValue((String)record2.value(), LinkedHashMap.class);

    Assertions.assertThat(payload).containsKey("header");
    Assertions.assertThat(payload).containsKey("event");
    Assertions.assertThat(payload).hasSize(2);

  }

  private String readResource(String path) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream istream = this.getClass().getResourceAsStream(path)) {
      byte[] buffer = new byte[8192];
      for (;;) {
        int r = istream.read(buffer);
        if (r<0) break;
        baos.write(buffer, 0, r);
      }
    }
    return new String(baos.toByteArray());
  }

}
