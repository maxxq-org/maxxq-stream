package org.maxxq.batch.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class BatchStream<T> implements IBatchStream<T> {
	private final Collection<Supplier<T>> collectionSupplier;
	private ExecutorService executorService;
	private int timeoutInSecondsForSuppliersToRespond = 60;
	
	public static <T> BatchStream<T> of(Collection<T> startingCollection) {
		Collection<Supplier<T>> collection = new ArrayList<Supplier<T>>();
		for (T item : startingCollection) {
			collection.add(() -> item);
		}
		return new BatchStream<T>(collection, null, 60);
	}

	private BatchStream(Collection<Supplier<T>> collectionSupplier, ExecutorService executorService, int timeoutForSuppliersToRespond) {
		this.collectionSupplier = collectionSupplier;
		this.executorService = executorService;
		this.timeoutInSecondsForSuppliersToRespond = timeoutForSuppliersToRespond;
	}

	@Override
	public <U> IBatchStream<U> map(Function<T, U> mapper) {
		return new BatchStream<U>(getSupplier(mapper), executorService, timeoutInSecondsForSuppliersToRespond);
	}

	@Override
	public IBatchStream<T> consume(Consumer<T> consumer) {
		return new BatchStream<T>(getSupplier(input -> {
			consumer.accept(input);
			return input;
		}), executorService, timeoutInSecondsForSuppliersToRespond);
	}

	private <U> Collection<Supplier<U>> getSupplier(Function<T, U> mapper) {
		Collection<Supplier<U>> newCollection = new ArrayList<Supplier<U>>();
		for (Supplier<T> item : collectionSupplier) {
			newCollection.add(() -> mapper.apply(item.get()));
		}
		return newCollection;
	}

	private void execute(Runnable runnable) {
		if (executorService == null) {
			runnable.run();
		} else {
			executorService.execute(runnable);
		}
	}

	@Override
	public Collection<T> collect() {
		Collection<T> result = new ArrayList<T>();
		CountDownLatch latch = new CountDownLatch(collectionSupplier.size());
		for (Supplier<T> supplier : collectionSupplier) {
			execute(() -> {
				try {
					result.add(supplier.get());
				} finally {
					latch.countDown();
				}
			});
		}
		try {
			if(!latch.await(timeoutInSecondsForSuppliersToRespond, TimeUnit.SECONDS)) {
				throw new TimeoutExpiredException("Not all providers responded within '" + timeoutInSecondsForSuppliersToRespond +" seconds");
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Could not wait till all suppliers responded", e);
		}
		return result;
	}

	@Override
	public BatchStream<T> parallel(ExecutorService exectorService) {
		this.executorService = exectorService;
		return this;
	}
	
	@Override
	public BatchStream<T> parallel(ExecutorService exectorService, int timeoutForSuppliersToRespond) {
		this.executorService = exectorService;
		this.timeoutInSecondsForSuppliersToRespond = timeoutForSuppliersToRespond;
		return this;
	}

	@Override
	public BatchStream<T> timeoutForSuppliersToRespond(int timeoutForSuppliersToRespond) {
		this.timeoutInSecondsForSuppliersToRespond = timeoutForSuppliersToRespond;
		return this;
	}
}
