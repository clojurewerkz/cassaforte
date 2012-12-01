package clojurewerkz.cassaforte.serializers;

import clojurewerkz.cassaforte.Serializer;

import java.nio.ByteBuffer;

public class IntegerSerializer extends AbstractSerializer<Integer> {

  @Override
  public ByteBuffer toByteBuffer(Integer obj) {
    if (obj == null) {
      return null;
    }

    ByteBuffer b = ByteBuffer.allocate(4);
    b.putInt(obj);
    b.rewind();
    return b;
  }

  @Override
  public Integer fromByteBuffer(ByteBuffer byteBuffer) {
    if ((byteBuffer == null) || (byteBuffer.remaining() != 4)) {
      return null;
    }
    int in = byteBuffer.getInt();
    byteBuffer.rewind();
    return in;
  }

  public static final IntegerSerializer instance = new IntegerSerializer();

  public static Serializer<?> getInstance() {
    return instance;
  }
}
