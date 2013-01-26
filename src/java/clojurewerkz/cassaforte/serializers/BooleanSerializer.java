package clojurewerkz.cassaforte.serializers;

import java.nio.ByteBuffer;

public class BooleanSerializer extends AbstractSerializer<Boolean> {


  @Override
  public ByteBuffer toByteBuffer(Boolean obj) {
    if (obj == null) {
      return null;
    }

    byte[] bytes = new byte[1];
    bytes[0] = obj ? (byte) 1 : (byte) 0;

    return ByteBuffer.wrap(bytes);
  }

  @Override
  public Boolean fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null || byteBuffer.remaining() < 1) {
      return null;
    }

    return byteBuffer.get() == (byte) 1;
  }
}
