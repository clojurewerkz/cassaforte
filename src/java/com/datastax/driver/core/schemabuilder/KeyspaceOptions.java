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

    boolean putSeparator = false;
    if (replication.isPresent()) {

      dropStatement.append("replication = {");


      int l = replication.get().entrySet().size();
      for (Map.Entry<String, Object> e: replication.get().entrySet()) {
        dropStatement.append("'" + e.getKey() + "'" + ": ");
        if (e.getValue() instanceof String) {
          dropStatement.append("'" + e.getValue() + "'");
        } else {
          dropStatement.append(e.getValue());
        }

        if (--l > 0) {
          dropStatement.append(", ");
        }
      }

			dropStatement.append('}');
      putSeparator = true;
    }



    if (durableWrites.isPresent()) {
      if (putSeparator) {
        dropStatement.append(" AND");
      }

      dropStatement.append(" DURABLE_WRITES = " + durableWrites.get().toString());
    }


    return dropStatement.toString();
  }
}
