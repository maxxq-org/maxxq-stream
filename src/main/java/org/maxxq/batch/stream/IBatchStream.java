package org.maxxq.batch.stream;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

public interface IBatchStream<T> {
	public <U> IBatchStream<U> map(Function<T, U> function);

	public IBatchStream<T> consume(Consumer<T> consumer);

	public Collection<T> collect();

	public BatchStream<T> parallel(ExecutorService exectorService);
	
	public BatchStream<T> timeoutForSuppliersToRespond(int timeoutForSuppliersToRespond);
	
	public BatchStream<T> parallel(ExecutorService exectorService, int timeoutForSuppliersToRespond);
}
