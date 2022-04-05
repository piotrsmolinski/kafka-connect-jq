package dev.psmolinski.kafka.connect.jq;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class JqTransformationConfig extends AbstractConfig  {

  public JqTransformationConfig(Map<?, ?> originals) {
    super(CONFIG_DEF, originals, false);
  }

  public String getQuery() {
    return getString("query");
  }

  public static ConfigDef CONFIG_DEF = buildConfigDef();

  private static ConfigDef buildConfigDef() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
            "query",
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "jq query expression"
    );
    return configDef;
  }
}
