public class ProcessorFactory {

    public static Processor create(TargetRetryPolicy targetRetryPolicy, long processingDelay) {
        var target = Config.SENDING_PROTOCOL.equals("grpc") ? new GrpcTarget(targetRetryPolicy)
            : new HttpTarget(targetRetryPolicy);
        return new Processor(target, processingDelay);
    }
}
