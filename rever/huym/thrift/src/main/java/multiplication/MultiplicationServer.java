package multiplication;

import multiplication.MultiplicationHandler;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.server.*;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.TFastFramedTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.transport.layered.TLayeredTransport;

import java.io.ByteArrayOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class MultiplicationServer {

  public static MultiplicationHandler handler;

  public static MultiplicationService.Processor processor;

  public static void main(String [] args) {
    try {
      handler = new MultiplicationHandler();
      processor = new MultiplicationService.Processor(handler);

      Runnable simple = new Runnable() {
        public void run() {
          simple(processor);
        }
      };      
     
      new Thread(simple).start();
    } catch (Exception x) {
      x.printStackTrace();
    }
  }

  public static void simple(MultiplicationService.Processor processor) {
    try {
      TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(9090);
//      TServerTransport serverTransport = new TServerSocket(9090);
      TServer server = new TThreadedSelectorServer(
//      TServer server = new THsHaServer(
//      TServer server = new TNonblockingServer(
//      TServer server = new TSimpleServer(
//      TServer server = new TThreadPoolServer(
              new TThreadedSelectorServer.Args(serverTransport)
              .processor(processor)
              .protocolFactory(
                      new TCompactProtocol.Factory()
//                      new TJSONProtocol.Factory()
//                      new TBinaryProtocol.Factory()
              )
              .transportFactory(
//                      new TFastFramedTransport.Factory()
                      new TFramedTransport.Factory()
              )
      );
//      TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
      System.out.println("Starting the server...");
      System.out.println("Listening....");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
