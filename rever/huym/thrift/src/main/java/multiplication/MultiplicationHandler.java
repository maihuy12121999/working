package multiplication;

import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.List;

public class MultiplicationHandler implements MultiplicationService.Iface {
	@Override
	public int multiply (int n1, int n2) throws TException {
		System.out.println("Server has received a multiplication request: " + n1 + "*" + n2);
		return n1*n2;
	}
}
