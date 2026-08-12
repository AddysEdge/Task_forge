package com.taskforge.validation;

import com.fasterxml.jackson.databind.JsonNode;
import com.taskforge.exception.PayloadValidationException;

/**
 * Small, strict readers for job payload fields.
 *
 * <p>Every handler validates through these so that a bad payload produces the same clear
 * message whether it is caught at submission time or at execution time, and so bounds are
 * never accidentally omitted.
 */
public final class PayloadFields {

    private PayloadFields() {
    }

    public static void requireObject(JsonNode payload) {
        if (payload == null || !payload.isObject()) {
            throw new PayloadValidationException("payload must be a JSON object");
        }
    }

    public static int requireInt(JsonNode payload, String field, int min, int max) {
        requireObject(payload);
        JsonNode node = payload.get(field);
        if (node == null || node.isNull()) {
            throw new PayloadValidationException("payload.%s is required".formatted(field));
        }
        return readInt(node, field, min, max);
    }

    public static int optionalInt(JsonNode payload, String field, int min, int max, int defaultValue) {
        requireObject(payload);
        JsonNode node = payload.get(field);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        return readInt(node, field, min, max);
    }

    private static int readInt(JsonNode node, String field, int min, int max) {
        if (!node.isIntegralNumber()) {
            throw new PayloadValidationException("payload.%s must be an integer".formatted(field));
        }
        long value = node.longValue();
        if (value < min || value > max) {
            throw new PayloadValidationException(
                    "payload.%s must be between %d and %d".formatted(field, min, max));
        }
        return (int) value;
    }

    public static String optionalText(JsonNode payload, String field, int maxLength, String defaultValue) {
        requireObject(payload);
        JsonNode node = payload.get(field);
        if (node == null || node.isNull()) {
            return defaultValue;
        }
        if (!node.isTextual()) {
            throw new PayloadValidationException("payload.%s must be a string".formatted(field));
        }
        String value = node.textValue();
        if (value.length() > maxLength) {
            throw new PayloadValidationException(
                    "payload.%s must be at most %d characters".formatted(field, maxLength));
        }
        return value;
    }

    public static <E extends Enum<E>> E optionalEnum(JsonNode payload, String field, Class<E> type, E defaultValue) {
        String raw = optionalText(payload, field, 64, null);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return Enum.valueOf(type, raw.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new PayloadValidationException("payload.%s must be one of %s".formatted(
                    field, java.util.Arrays.toString(type.getEnumConstants())));
        }
    }
}
