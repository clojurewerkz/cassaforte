package clojurewerkz.cassaforte;

import java.nio.ByteBuffer;

public interface Serializer<T> {
  ByteBuffer toByteBuffer(T obj);
  T fromByteBuffer(ByteBuffer byteBuffer);

  byte[] toBytes(T obj);
  T fromBytes(byte[] bytes);
}