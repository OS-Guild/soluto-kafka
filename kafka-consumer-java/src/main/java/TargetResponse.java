import java.util.OptionalLong;

enum TargetResponseType {
    Success,
    Error
}

public class TargetResponse {
    public TargetResponseType type;
    public OptionalLong callLatency;
    public Throwable exception;

    private TargetResponse(TargetResponseType targetResponseType, Throwable exception) {
        this.type = targetResponseType;
        this.exception = exception;
    }

    TargetResponse(TargetResponseType targetResponseType) {
        this.type = targetResponseType;
    }

    TargetResponse(TargetResponseType targetResponseType, OptionalLong callLatency) {
        this.type = targetResponseType;
        this.callLatency = callLatency;
    }    

    public static TargetResponse Success = new TargetResponse(TargetResponseType.Success);

    public static TargetResponse Error(Throwable t) {
        return new TargetResponse(TargetResponseType.Error, t);
    } 



}