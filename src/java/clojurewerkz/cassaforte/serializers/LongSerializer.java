package clojurewerkz.cassaforte.serializers;

import clojurewerkz.cassaforte.Serializer;

import java.nio.ByteBuffer;

public class LongSerializer extends AbstractSerializer<Long> {

  @Override
  public ByteBuffer toByteBuffer(Long obj) {
    if (obj == null) {
      return null;
    }
    return ByteBuffer.allocate(8).putLong(0, obj);
  }

  @Override
  public Long fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    else if (byteBuffer.remaining() == 8) {
      return byteBuffer.getLong();
    }
    else if (byteBuffer.remaining() == 4) {
      return (long) byteBuffer.getInt();
    }
    return null;
  }
}
