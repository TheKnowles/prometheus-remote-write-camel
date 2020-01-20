package org.camel.prometheus.example;

import com.google.protobuf.util.JsonFormat;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.converter.stream.InputStreamCache;
import org.apache.camel.dataformat.protobuf.ProtobufDataFormat;
import org.xerial.snappy.Snappy;
import prometheus.Remote;

import java.io.ByteArrayOutputStream;

public class MyRouteBuilder extends RouteBuilder {

    private static JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();

    public void configure() {
        ProtobufDataFormat format = new ProtobufDataFormat(Remote.WriteRequest.getDefaultInstance());

        from("jetty:http://localhost:8000/receive")
                .log("Received message on /receive endpoint")
                .process(exchange -> {
                    InputStreamCache cache = (InputStreamCache)exchange.getIn().getBody();
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int nRead;
                    byte[] data = new byte[1024];
                    while ((nRead = cache.read(data, 0, data.length)) != -1) {
                        buffer.write(data, 0, nRead);
                    }
                    buffer.flush();
                    exchange.getMessage().setBody(Snappy.uncompress(buffer.toByteArray()));
                })
                .unmarshal(format)
                .process(exchange -> {
                    String json = JSON_PRINTER.print((Remote.WriteRequest)exchange.getIn().getBody());
                    exchange.getMessage().setBody(json);
                })
        .to("file://target/data_samples");
    }
}
