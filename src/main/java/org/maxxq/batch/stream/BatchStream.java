package org.maxxq.batch.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class BatchStream<T> implements IBatchStream<T> {
	private final Supplier<Collection<T>> collectionSupplier;
	private ExecutorService execturService;

	public static <T> BatchStream<T> of(Collection<T> startingCollection) {
		return new BatchStream<T>(() -> startingCollection);
	}

	private BatchStream(Supplier<Collection<T>> collectionSupplier) {
		this.collectionSupplier = collectionSupplier;
	}

	@Override
	public <U> IBatchStream<U> map(Function<T, U> mapper) {
		return new BatchStream<U>(getSupplier(mapper));
	}

	@Override
	public IBatchStream<T> consume(Consumer<T> consumer) {
		return new BatchStream<T>(getSupplier(input -> {
			consumer.accept(input);
			return input;
		}));
	}

	private <U> Supplier<Collection<U>> getSupplier(Function<T, U> mapper) {
		return () -> {
			Collection<T> originalCollection = collectionSupplier.get();
			CountDownLatch latch = new CountDownLatch(originalCollection.size());
			Collection<U> newCollection = new ArrayList<U>();
			for (T item : originalCollection) {
				execute(() -> {
					try {
						newCollection.add(mapper.apply(item));
					} finally {
						latch.countDown();
					}
				});
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			return newCollection;
		};

	}

	private void execute(Runnable runnable) {
		if (execturService == null) {
			runnable.run();
		} else {
			execturService.execute(runnable);
		}
	}

	@Override
	public Collection<T> collect() {
		return collectionSupplier.get();
	}

	@Override
	public BatchStream<T> parallel(ExecutorService exectorService) {
		this.execturService = exectorService;
		return this;
	}
}
