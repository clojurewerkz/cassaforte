package clojurewerkz.cassaforte.serializers;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class AsciiSerializer extends AbstractSerializer<String> {

  private static final Charset charset = Charset.forName("US-ASCII");

  @Override
  public ByteBuffer toByteBuffer(String obj) {
    if (obj == null) {
      return null;
    }

    return  ByteBuffer.wrap(obj.getBytes(charset));
  }

  @Override
  public String fromByteBuffer(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }
    return charset.decode(byteBuffer).toString();
  }
}
