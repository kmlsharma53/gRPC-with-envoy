package json;

import static io.grpc.stub.ClientCalls.blockingUnaryCall;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Advanced example of how to swap out the serialization logic.  Normal users do not need to do
 * this.  This code is not intended to be a production-ready implementation, since JSON encoding
 * is slow.  Additionally, JSON serialization as implemented may be not resilient to malicious
 * input.
 *
 * <p>If you are considering implementing your own serialization logic, contact the grpc team at
 * https://groups.google.com/forum/#!forum/grpc-io
 */
public final class HelloJsonClient {
  private static final Logger logger = Logger.getLogger(HelloJsonClient.class.getName());

  private final ManagedChannel channel;
  private final HelloJsonStub blockingStub;

  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  public HelloJsonClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port)
        .usePlaintext()
        .build();
    blockingStub = new HelloJsonStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Say hello to server. */
  public void greet(String name) {
    HelloRequest request = new HelloRequest();
    request.setName(name);
    HelloReply response;
    try {
      response = blockingStub.sayHello(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }
    logger.info(response.getMessage());
  }

  private static void runPerformanceTest(Runnable runnable){

      long time1 = System.currentTimeMillis();
      for (int i = 0; i < 900000; i++) {
        runnable.run();
      }
      long time2 = System.currentTimeMillis();

      System.out.println(
              "JSON" + " : " + (time2 - time1) + " ms"
      );

  }

  public static void main(String[] args) throws Exception {
    HelloJsonClient client = new HelloJsonClient("127.0.0.1", 15001);
    try {

      final String user = "TEST ANANABANBANBANBANABNABANA ANBANABANBANABAN";

      client.greet(user);
      //runPerformanceTest(runnable);
    } finally {
      client.shutdown();
    }
  }

  static final class HelloJsonStub extends AbstractStub<HelloJsonStub> {

    static final MethodDescriptor<HelloRequest, HelloReply> METHOD_SAY_HELLO =

              MethodDescriptor.newBuilder(
              marshallerFor(HelloRequest.class),
              marshallerFor(HelloReply.class))
                      .setFullMethodName("Greeter/SayHello")
                      .setType(MethodDescriptor.MethodType.UNARY)
                    .setSampledToLocalTracing(true)
                    .build();

    protected HelloJsonStub(Channel channel) {
      super(channel);
    }

    protected HelloJsonStub(Channel channel, CallOptions callOptions) {
      super(channel, callOptions);
    }

    @Override
    protected HelloJsonStub build(Channel channel, CallOptions callOptions) {
      return new HelloJsonStub(channel, callOptions);
    }

    public HelloReply sayHello(HelloRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SAY_HELLO, getCallOptions(), request);
    }
  }

  private static final Gson gson =
    new GsonBuilder().registerTypeAdapter(byte[].class, new TypeAdapter<byte[]>() {
      public void write(JsonWriter out, byte[] value) throws IOException {
        out.value(Base64.getEncoder().encodeToString(value));
      }

      @Override
      public byte[] read(JsonReader in) throws IOException {
        return Base64.getDecoder().decode(in.nextString());
      }
    }).create();
  static <T> MethodDescriptor.Marshaller<T> marshallerFor(Class<T> clz) {
    return new MethodDescriptor.Marshaller<T>() {
      public InputStream stream(T value) {
        return new ByteArrayInputStream(gson.toJson(value, clz).getBytes(StandardCharsets.UTF_8));
      }

      public T parse(InputStream stream) {
        return gson.fromJson(new InputStreamReader(stream, StandardCharsets.UTF_8), clz);
      }
    };
  }

}