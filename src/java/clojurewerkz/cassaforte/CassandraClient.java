package clojurewerkz.cassaforte;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class CassandraClient {
  private Cassandra.Client client;
  private TTransport transport;

  private static final int DEFAULT_PORT = 9160;


  //
  // Constructors
  //

  public CassandraClient(Cassandra.Client thriftClient, TTransport thriftTransport) {
    client = thriftClient;
    transport = thriftTransport;
  }

  public CassandraClient(String hostname, String keyspace) throws TException, InvalidRequestException {
    this(hostname, DEFAULT_PORT, keyspace);
  }

  public CassandraClient(String hostname, int port, String keyspace) throws TException, InvalidRequestException {
    TTransport tr = new TFramedTransport(new TSocket(hostname, port));

    transport = tr;
    client = new Cassandra.Client(new TBinaryProtocol(tr));
    tr.open();
    client.set_keyspace(keyspace);
  }

  //
  // Thrift Client delegates
  //

  public void login(AuthenticationRequest auth_request) throws AuthenticationException, AuthorizationException, TException {
    client.login(auth_request);
  }

  public void set_keyspace(String keyspace) throws InvalidRequestException, TException {
    client.set_keyspace(keyspace);
  }

  public ColumnOrSuperColumn get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level) throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException, TException {
    return client.get(key, column_path, consistency_level);
  }

  public int get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    return client.get_count(key, column_parent, predicate, consistency_level);
  }

  public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    return client.get_slice(key, column_parent, predicate, consistency_level);
  }

  public Map<ByteBuffer, Integer> multiget_count(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    return client.multiget_count(keys, column_parent, predicate, consistency_level);
  }

  public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    return client.multiget_slice(keys, column_parent, predicate, consistency_level);
  }

  public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    client.insert(key, column_parent, column, consistency_level);
  }

  public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    return client.get_range_slices(column_parent, predicate, range, consistency_level);
  }

  public List<KeySlice> get_paged_slice(String column_family, KeyRange range, ByteBuffer start_column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    return client.get_paged_slice(column_family, range, start_column, consistency_level);
  }

  public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    return client.get_indexed_slices(column_parent, index_clause, column_predicate, consistency_level);
  }

  public CqlResult execute_prepared_cql_query(int itemId, List<ByteBuffer> values) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
    return client.execute_prepared_cql_query(itemId, values);
  }

  public CqlResult execute_cql_query(ByteBuffer query, Compression compression) throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException {
    return client.execute_cql_query(query, compression);
  }

  public String describe_version() throws TException {
    return client.describe_version();
  }

  public String describe_snitch() throws TException {
    return client.describe_snitch();
  }

  public String describe_cluster_name() throws TException {
    return client.describe_cluster_name();
  }

  public KsDef describe_keyspace(String keyspace) throws NotFoundException, InvalidRequestException, TException {
    return client.describe_keyspace(keyspace);
  }

  public List<KsDef> describe_keyspaces() throws InvalidRequestException, TException {
    return client.describe_keyspaces();
  }

  public String describe_partitioner() throws TException {
    return client.describe_partitioner();
  }

  public List<TokenRange> describe_ring(String keyspace) throws InvalidRequestException, TException {
    return client.describe_ring(keyspace);
  }

  public Map<String, List<String>> describe_schema_versions() throws InvalidRequestException, TException {
    return client.describe_schema_versions();
  }

  public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split) throws InvalidRequestException, TException {
    return client.describe_splits(cfName, start_token, end_token, keys_per_split);
  }

  public void add(ByteBuffer key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    client.add(key, column_parent, column, consistency_level);
  }

  public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    client.batch_mutate(mutation_map, consistency_level);
  }

  public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression) throws InvalidRequestException, TException {
    return client.prepare_cql_query(query, compression);
  }

  public void remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    client.remove(key, column_path, timestamp, consistency_level);
  }

  public void remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    client.remove_counter(key, path, consistency_level);
  }

  public String system_add_column_family(CfDef cf_def) throws InvalidRequestException, SchemaDisagreementException, TException {
    return client.system_add_column_family(cf_def);
  }

  public String system_add_keyspace(KsDef ks_def) throws InvalidRequestException, SchemaDisagreementException, TException {
    return client.system_add_keyspace(ks_def);
  }

  public String system_drop_column_family(String column_family) throws InvalidRequestException, SchemaDisagreementException, TException {
    return client.system_drop_column_family(column_family);
  }

  public String system_drop_keyspace(String keyspace) throws InvalidRequestException, SchemaDisagreementException, TException {
    return client.system_drop_keyspace(keyspace);
  }

  public String system_update_column_family(CfDef cf_def) throws InvalidRequestException, SchemaDisagreementException, TException {
    return client.system_update_column_family(cf_def);
  }

  public String system_update_keyspace(KsDef ks_def) throws InvalidRequestException, SchemaDisagreementException, TException {
    return client.system_update_keyspace(ks_def);
  }

  public void truncate(String cfname) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
    client.truncate(cfname);
  }


  //
  // Transport delegates
  //

  public void open() throws TTransportException {
    transport.open();
  }

  public void close() {
    transport.close();
  }

  public void flush() throws TTransportException {
    transport.flush();
  }

  public boolean isOpen() {
    return transport.isOpen();
  }
}
