package org.maxxq.batch.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class BatchStream<T> implements IBatchStream<T> {
	private final Collection<Supplier<T>> collectionSupplier;
	private ExecutorService executorService;
	private int maxProcessingTime;

	public static <T> BatchStream<T> of(Collection<T> startingCollection) {
		Collection<Supplier<T>> collection = new ArrayList<Supplier<T>>();
		for (T item : startingCollection) {
			collection.add(() -> item);
		}
		return new BatchStream<T>(collection, null, Integer.MAX_VALUE);
	}

	private BatchStream(Collection<Supplier<T>> collectionSupplier, ExecutorService executorService, int maxProcessingTime) {
		this.collectionSupplier = collectionSupplier;
		this.executorService = executorService;
		this.maxProcessingTime = maxProcessingTime;
	}

	@Override
	public <U> IBatchStream<U> map(Function<T, U> mapper) {
		return new BatchStream<U>(getSupplier(mapper), executorService, maxProcessingTime);
	}

	@Override
	public IBatchStream<T> consume(Consumer<T> consumer) {
		return new BatchStream<T>(getSupplier(input -> {
			consumer.accept(input);
			return input;
		}), executorService, maxProcessingTime);
	}

	private <U> Collection<Supplier<U>> getSupplier(Function<T, U> mapper) {
		Collection<Supplier<U>> newCollection = new ArrayList<Supplier<U>>();
		for (Supplier<T> item : collectionSupplier) {
			newCollection.add(() -> mapper.apply(item.get()));
		}
		return newCollection;
	}

	@Override
	public <Z extends Collection<T>> Z collect(Z collection) {
		Collection<T> threadSafeCollection = Collections.synchronizedCollection(collection);
		CountDownLatch latch = new CountDownLatch(collectionSupplier.size());
		for (Supplier<T> supplier : collectionSupplier) {
			execute(() -> {
				try {
					threadSafeCollection.add(supplier.get());
				} finally {
					latch.countDown();
				}
			});
		}
		try {
			if (!latch.await(maxProcessingTime, TimeUnit.SECONDS)) {
				throw new BatchStreamException(
						"Collecting failed within " + maxProcessingTime + " seconds, " + latch.getCount() + " items are not processed, closing executor service now");
			}
		} catch (InterruptedException e) {
			throw new BatchStreamException("Could not wait till all suppliers responded", e);
		} finally {
			if (executorService != null) {
				executorService.shutdownNow();
			}
		}
		return collection;
	}

	private void execute(Runnable runnable) {
		if (executorService == null) {
			runnable.run();
		} else {
			executorService.execute(runnable);
		}
	}

	@Override
	public BatchStream<T> parallel(ExecutorService exectorService) {
		this.executorService = exectorService;
		return this;
	}

	@Override
	public BatchStream<T> maxProcessingTimeInSeconds(int maxProcessingTime) {
		if (executorService == null) {
			throw new IllegalArgumentException("Specifying the max processing time is only relevant if an executor service is defined");
		}
		this.maxProcessingTime = maxProcessingTime;
		return this;
	}
}
