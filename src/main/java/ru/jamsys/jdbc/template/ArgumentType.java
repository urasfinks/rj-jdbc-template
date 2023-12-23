package ru.jamsys.jdbc.template;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public enum ArgumentType {
    CLOB,
    VARCHAR,
    NUMBER,
    TIMESTAMP,

    IN_ENUM_VARCHAR,
    IN_ENUM_TIMESTAMP,
    IN_ENUM_CLOB,
    IN_ENUM_NUMBER;

    static final Function<Object, String> enumInFunction = object -> {
        if (object instanceof List) {
            List listArgs = (List) object;
            if (listArgs.isEmpty()) {
                throw new RuntimeException("enumInFunction list is empty");
            }
            String[] array = new String[listArgs.size()];
            Arrays.fill(array, "?");
            return String.join(",", array);
        }
        throw new RuntimeException("enumInFunction object: " + object + " is not List");
    };

    static final Map<ArgumentType, Function<Object, String>> dynamicType = new HashMap<>() {{
        put(IN_ENUM_VARCHAR, enumInFunction);
        put(IN_ENUM_TIMESTAMP, enumInFunction);
        put(IN_ENUM_CLOB, enumInFunction);
        put(IN_ENUM_NUMBER, enumInFunction);
    }};

    @SuppressWarnings("unused")
    ArgumentType getRealType() {
        return switch (this) {
            case CLOB, IN_ENUM_CLOB -> CLOB;
            case NUMBER, IN_ENUM_NUMBER -> NUMBER;
            case TIMESTAMP, IN_ENUM_TIMESTAMP -> TIMESTAMP;
            case VARCHAR, IN_ENUM_VARCHAR -> VARCHAR;
        };
    }

    public boolean isString() {
        return this == CLOB || this == VARCHAR;
    }

    String compileDynamicFragment(Object obj, String information) {
        if (dynamicType.containsKey(this)) {
            try {
                return dynamicType.get(this).apply(obj);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage() + " (" + information + ")");
            }
        }
        throw new RuntimeException("FragmentType (" + this.name() + ") function does not exist");
    }

    boolean isDynamicFragment() {
        return dynamicType.containsKey(this);
    }
}
