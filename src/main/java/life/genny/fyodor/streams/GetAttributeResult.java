package life.genny.fyodor.streams;

import java.util.Optional;
import java.util.OptionalInt;

import life.genny.qwandaq.attribute.Attribute;

public class GetAttributeResult {

    private static GetAttributeResult NOT_FOUND = new GetAttributeResult(null);

    private final Attribute result;

    private GetAttributeResult(Attribute result) {
        this.result = result;
    }

    public static GetAttributeResult found(Attribute data) {
        return new GetAttributeResult(data);
    }

    public static GetAttributeResult notFound() {
        return NOT_FOUND;
    }

    public Optional<Attribute> getResult() {
        return Optional.ofNullable(result);
    }
}
