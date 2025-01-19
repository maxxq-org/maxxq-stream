package org.maxxq.batch.stream;

public class TimeoutExpiredException extends RuntimeException {
	private static final long serialVersionUID = -1756690056109994757L;

	public TimeoutExpiredException(String message) {
		super(message);
	}
}
