package com.bazaarvoice.nn.nataraja.athenadataharvester.exception;


public class TaskFailureException extends Exception {

    public TaskFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskFailureException(String message) {
        super(message);
    }
}
