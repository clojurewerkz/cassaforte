package clojurewerkz.cassaforte.serializers;

import java.math.BigInteger;
import java.nio.ByteBuffer;

public class BigIntegerSerializer extends AbstractSerializer<BigInteger> {

  @Override
  public ByteBuffer toByteBuffer(BigInteger obj) {
    return ByteBuffer.wrap(obj.toByteArray());
  }

  @Override
  public BigInteger fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }

    int length = byteBuffer.remaining();
    byte[] bytes = new byte[length];
    return new BigInteger(bytes);
  }
}
