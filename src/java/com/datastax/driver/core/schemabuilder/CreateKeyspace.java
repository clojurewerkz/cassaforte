package com.datastax.driver.core.schemabuilder;

/**
 *
 */
public class CreateKeyspace extends SchemaStatement {

  private final String  keyspaceName;
  private       boolean ifNotExists;

  public CreateKeyspace(String keyspaceName) {
    this.keyspaceName = keyspaceName;
    this.ifNotExists = false;
  }

  public CreateKeyspace ifNotExists() {
    this.ifNotExists = true;
    return this;
  };

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
    StringBuilder createStatement = new StringBuilder(STATEMENT_START).append("CREATE KEYSPACE ");
    if (ifNotExists) {
      createStatement.append("IF NOT EXISTS ");
    }
    createStatement.append(keyspaceName);
    return createStatement.toString();
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
