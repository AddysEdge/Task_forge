package com.taskforge.exception;

import com.taskforge.dto.ApiError;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.List;

/**
 * Turns every failure into the same {@link ApiError} shape.
 *
 * <p>Two rules: clients get a message they can act on, and they never get an
 * implementation detail. Stack traces, SQL text and exception class names stay in the
 * server log, which is where the full context is written instead.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleBodyValidation(MethodArgumentNotValidException e,
                                                         HttpServletRequest request) {
        List<ApiError.FieldViolation> violations = e.getBindingResult().getFieldErrors().stream()
                .map(error -> new ApiError.FieldViolation(error.getField(), error.getDefaultMessage()))
                .toList();

        String message = violations.isEmpty()
                ? "Request validation failed"
                : violations.get(0).field() + ": " + violations.get(0).message();

        return build(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", message, request, violations);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ApiError> handleParameterValidation(ConstraintViolationException e,
                                                              HttpServletRequest request) {
        List<ApiError.FieldViolation> violations = e.getConstraintViolations().stream()
                .map(violation -> new ApiError.FieldViolation(
                        violation.getPropertyPath().toString(), violation.getMessage()))
                .toList();
        return build(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", "Request validation failed", request, violations);
    }

    @ExceptionHandler(PayloadValidationException.class)
    public ResponseEntity<ApiError> handlePayloadValidation(PayloadValidationException e,
                                                            HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "PAYLOAD_VALIDATION_ERROR", e.getMessage(), request, null);
    }

    /** Malformed JSON, or a value that does not fit the target type (e.g. an unknown jobType). */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadableBody(HttpMessageNotReadableException e,
                                                         HttpServletRequest request) {
        log.debug("Rejected unreadable request body on {}: {}", request.getRequestURI(), e.getMessage());
        return build(HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST",
                "Request body is not valid JSON, or a field has an unsupported value", request, null);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiError> handleTypeMismatch(MethodArgumentTypeMismatchException e,
                                                       HttpServletRequest request) {
        return build(HttpStatus.BAD_REQUEST, "INVALID_PARAMETER",
                "Parameter '%s' has an invalid value".formatted(e.getName()), request, null);
    }

    @ExceptionHandler(JobNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(JobNotFoundException e, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, "JOB_NOT_FOUND", e.getMessage(), request, null);
    }

    /**
     * An unmatched path. Both exceptions mean the same thing to a caller, but Spring raises
     * {@link NoResourceFoundException} for URLs that fall through to static resource
     * handling — which is every unknown path, including actuator endpoints that are
     * deliberately not exposed. Without this they would reach the catch-all below and be
     * reported as 500.
     */
    @ExceptionHandler({NoHandlerFoundException.class, NoResourceFoundException.class})
    public ResponseEntity<ApiError> handleNoHandler(Exception e, HttpServletRequest request) {
        return build(HttpStatus.NOT_FOUND, "NOT_FOUND", "No endpoint for this path", request, null);
    }

    @ExceptionHandler(JobConflictException.class)
    public ResponseEntity<ApiError> handleConflict(JobConflictException e, HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, "JOB_STATE_CONFLICT", e.getMessage(), request, null);
    }

    @ExceptionHandler(InvalidStateTransitionException.class)
    public ResponseEntity<ApiError> handleInvalidTransition(InvalidStateTransitionException e,
                                                            HttpServletRequest request) {
        return build(HttpStatus.CONFLICT, "INVALID_STATE_TRANSITION", e.getMessage(), request, null);
    }

    /**
     * Someone else changed the row first. Reported rather than retried silently: the caller
     * knows whether repeating the operation is safe, and this service does not.
     */
    @ExceptionHandler(OptimisticLockingFailureException.class)
    public ResponseEntity<ApiError> handleOptimisticLock(OptimisticLockingFailureException e,
                                                         HttpServletRequest request) {
        log.warn("Optimistic lock conflict on {}: {}", request.getRequestURI(), e.getMessage());
        return build(HttpStatus.CONFLICT, "CONCURRENT_MODIFICATION",
                "The job was modified concurrently; re-read it and retry", request, null);
    }

    @ExceptionHandler(DataAccessResourceFailureException.class)
    public ResponseEntity<ApiError> handleDatastoreDown(DataAccessResourceFailureException e,
                                                        HttpServletRequest request) {
        log.error("Datastore unavailable handling {}: {}", request.getRequestURI(), e.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE",
                "A required datastore is currently unavailable", request, null);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception e, HttpServletRequest request) {
        // Full detail to the log, a generic message to the caller.
        log.error("Unhandled exception on {} {}", request.getMethod(), request.getRequestURI(), e);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "An unexpected error occurred", request, null);
    }

    private ResponseEntity<ApiError> build(HttpStatus status,
                                           String error,
                                           String message,
                                           HttpServletRequest request,
                                           List<ApiError.FieldViolation> violations) {
        return ResponseEntity.status(status)
                .body(ApiError.of(status.value(), error, message, request.getRequestURI(), violations));
    }
}
