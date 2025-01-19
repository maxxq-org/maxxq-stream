package org.maxxq.batch.stream;

public class BatchStreamException extends RuntimeException {
	private static final long serialVersionUID = -1756690056109994757L;

	public BatchStreamException(String message) {
		super(message);
	}

	public BatchStreamException(String message, InterruptedException e) {
		super(message, e);
	}
}
