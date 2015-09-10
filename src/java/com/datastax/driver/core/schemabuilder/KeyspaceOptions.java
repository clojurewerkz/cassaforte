package com.datastax.driver.core.schemabuilder;

import com.google.common.base.Optional;

import java.util.Map;

public class KeyspaceOptions extends SchemaStatement {

  private Optional<Map<String, Object>> replication   = Optional.absent();
  private Optional<Boolean>             durableWrites = Optional.absent();

  public KeyspaceOptions() {

  }

  public KeyspaceOptions replication(Map<String, Object> replication) {
    this.replication = Optional.of(replication);
    return this;
  }

  public KeyspaceOptions durableWrites(boolean durableWrites){
    this.durableWrites = Optional.of(durableWrites);
    return this;
  }

  @Override
  String buildInternal() {
    StringBuilder dropStatement = new StringBuilder(" ");

    boolean putComma = false;
    if (replication.isPresent()) {
      dropStatement.append("REPLICATION = {");


      int l = replication.get().entrySet().size();
      for (Map.Entry<String, Object> e: replication.get().entrySet()) {
        dropStatement.append(e.getKey() + ": ");
        if (e.getValue() instanceof String) {
          dropStatement.append(" '" + e.getValue() + "'");
        } else {
          dropStatement.append(e.getValue());
        }

        if (--l > 0) {
          dropStatement.append(", ");
        }
      }

      putComma = true;
    }

    if (durableWrites.isPresent()) {
      if (putComma) {
        dropStatement.append(',');
      }

      dropStatement.append(" DURABLE_WRITES = " + durableWrites.get().toString());
    }

    dropStatement.append('}');
    return dropStatement.toString();
  }
}
