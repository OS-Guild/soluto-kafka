package target;

import java.util.OptionalLong;

enum TargetResponseType {
    Success, Error
}

public class TargetResponse {
    public TargetResponseType type;
    public OptionalLong callLatency;
    public OptionalLong resultLatency;
    public Throwable exception;

    private TargetResponse(Throwable exception) {
        this.type = TargetResponseType.Error;
        this.exception = exception;
        this.callLatency = OptionalLong.empty();
        this.resultLatency = OptionalLong.empty();
    }

    TargetResponse(TargetResponseType targetResponseType, OptionalLong callLatency, OptionalLong resultLatency) {
        this.type = targetResponseType;
        this.callLatency = callLatency;
        this.resultLatency = resultLatency;
    }

    public static TargetResponse Error(Throwable t) {
        return new TargetResponse(t);
    }
}