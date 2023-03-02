public class test {
    private static int intToZigZag(int n) {
        return n << 1 ^n>>31;
    }
    public static void main(String[] args) {
        int result = intToZigZag(50399);
        System.out.println(result);
    }

}
