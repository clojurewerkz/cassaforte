package com.datastax.driver.core.schemabuilder;

/**
 * A built DROP KEYSPACE statement.
 */
public class DropKeyspace extends SchemaStatement {

  private final String keyspaceName;
  private       boolean ifExists = false;

  public DropKeyspace(String keyspaceName) {
    this.keyspaceName = keyspaceName;
    validateNotEmpty(keyspaceName, "Keyspace name");
    validateNotKeyWord(keyspaceName, String.format("The keyspace name '%s' is not allowed because it is a reserved keyword", keyspaceName));
  }

  /**
   * Add the 'IF EXISTS' condition to this DROP statement.
   *
   * @return this statement.
   */
  public DropKeyspace ifExists() {
    this.ifExists = true;
    return this;
  }

  @Override
  public String buildInternal() {
    StringBuilder dropStatement = new StringBuilder("DROP KEYSPACE ");
    if (ifExists) {
      dropStatement.append("IF EXISTS ");
    }
		dropStatement.append(keyspaceName);
    dropStatement.append(';');
    return dropStatement.toString();
  }

  /**
   * Generate a DROP TABLE statement
   * @return the final DROP TABLE statement
   */
  public String build() {
    return this.buildInternal();
  }
}
