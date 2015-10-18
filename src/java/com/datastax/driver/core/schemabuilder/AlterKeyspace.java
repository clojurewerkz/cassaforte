package com.datastax.driver.core.schemabuilder;

/**
 *
 */
public class AlterKeyspace extends SchemaStatement {

  private final String  keyspaceName;

  public AlterKeyspace(String keyspaceName) {
    this.keyspaceName = keyspaceName;
  }

  public Options withOptions() {
    return new Options(this);
  }

  @Override
  public String getQueryString() {
    String built = super.getQueryString();
    return built + ";";
  }

  @Override
  String buildInternal() {
    StringBuilder alterStatement = new StringBuilder(STATEMENT_START).append("ALTER KEYSPACE ");
    alterStatement.append(keyspaceName);
    return alterStatement.toString();
  }

  public static class Options extends KeyspaceOptions {

    private final SchemaStatement statement;

    public Options(SchemaStatement statement) {
      this.statement = statement;
    }

    @Override
    String buildInternal() {
      StringBuilder renderedStatement = new StringBuilder(statement.buildInternal());
      renderedStatement.append(" WITH");
      renderedStatement.append(super.buildInternal());
      renderedStatement.append(";");
      return renderedStatement.toString();

    }
  }
}
