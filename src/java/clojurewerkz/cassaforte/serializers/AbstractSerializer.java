package clojurewerkz.cassaforte.serializers;

import clojurewerkz.cassaforte.Serializer;

import java.nio.ByteBuffer;

public abstract class AbstractSerializer<T> implements Serializer<T> {

  @Override
  public abstract ByteBuffer toByteBuffer(T obj);

  @Override
  public abstract T fromByteBuffer(ByteBuffer byteBuffer);

  @Override
  public byte[] toBytes(T obj) {
    ByteBuffer bb = toByteBuffer(obj);
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes, 0, bytes.length);
    return bytes;
  }

  @Override
  public T fromBytes(byte[] bytes) {
    return fromByteBuffer(ByteBuffer.wrap(bytes));
  }

}
