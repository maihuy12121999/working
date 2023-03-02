package multiplication;

import com.sun.org.apache.xalan.internal.utils.ConfigurationError;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.util.Arrays;

public class MultiplicationClient {
  public static void main(String [] args) {
    try {

      TTransport transport = new TSocket("localhost", 9090);
//      transport = new TZlibTransport(transport);
      transport = new TFramedTransport(transport);
//      transport = new TTransport(transport);

//      TProtocol protocol = new TBinaryProtocol(transport);
      TProtocol protocol = new TCompactProtocol(transport);
//      TProtocol protocol = new TJSONProtocol(transport);
      MultiplicationService.Client client = new MultiplicationService.Client(protocol);
      transport.open();
      perform(client);
      transport.close();
    } catch (TException x) {
      x.printStackTrace();
    }

  }

  private static void perform(MultiplicationService.Client client) throws TException
  {
    int product = client.multiply(1,9);
    System.out.println("Server has response. Result is " + product);
  }
}

