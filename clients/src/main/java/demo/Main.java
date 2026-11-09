package demo;

/** Entry point: producer | consumer | audit &lt;pri|sec&gt; &lt;label&gt; | offsets &lt;pri|sec&gt; */
public final class Main {
    public static void main(String[] args) throws Exception {
        Common.initJvmTrust();
        String mode = args.length > 0 ? args[0] : Common.env("MODE", "");
        switch (mode) {
            case "producer" -> new Producer().run();
            case "consumer" -> new Consumer().run();
            case "audit" -> Audit.audit(args[1], args[2]);
            case "offsets" -> Audit.offsets(args[1]);
            default -> {
                System.err.println("usage: producer | consumer | audit <pri|sec> <label> | offsets <pri|sec>");
                System.exit(2);
            }
        }
        System.exit(0);
    }
}
