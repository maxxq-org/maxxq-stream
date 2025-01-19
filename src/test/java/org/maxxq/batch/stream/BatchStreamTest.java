package org.maxxq.batch.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

public class BatchStreamTest {
	@Test
	@RepeatedTest(20)
	public void collectParallel() {
		List<Integer> result = BatchStream
				.of(Arrays.asList("1", "2", "3"))
				.parallel(Executors.newFixedThreadPool(3))
				.map(numericString -> numericString + "0")
				.map(someString -> Integer.parseInt(someString))
				.consume(number -> sleep(200 - 2 * number))
				.collect(new ArrayList<Integer>());

		result.forEach(number -> System.out.println(number));

		// because of the parallel processing and sleep time, results will endup in the
		// opposite order
		// this proves parallel processing really took place and the resulting
		// collection can have a different
		// order than the input collection
		Assertions.assertThat(result.size()).isEqualTo(3);
		Assertions.assertThat(result.get(0)).isEqualTo(30);
		Assertions.assertThat(result.get(1)).isEqualTo(20);
		Assertions.assertThat(result.get(2)).isEqualTo(10);
	}

	@Test
	public void collect() {
		List<Integer> result = BatchStream
				.of(Arrays.asList("1", "2", "3"))
				.map(numericString -> numericString + "0")
				.map(someString -> Integer.parseInt(someString))
				.consume(number -> sleep(200 - 2 * number))
				.collect(new ArrayList<Integer>());

		result.forEach(number -> System.out.println(number));

		// this stream is not parallel, even though there are sleep times, the expected
		// order is the same as the input order
		Assertions.assertThat(result.size()).isEqualTo(3);
		Assertions.assertThat(result.get(0)).isEqualTo(10);
		Assertions.assertThat(result.get(1)).isEqualTo(20);
		Assertions.assertThat(result.get(2)).isEqualTo(30);
	}

	private void sleep(long sleepTime) {
		try {
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
		}
	}

	@Test
	public void collectNotFinishedWithinProcessingTime() {
		Assertions
				.assertThatThrownBy(() -> BatchStream
						.of(Arrays.asList("1", "2", "3"))
						.parallel(Executors.newFixedThreadPool(1))
						.maxProcessingTimeInSeconds(1)
						.consume(numberAsString -> sleep(2000))
						.collect(new ArrayList<>()))
				.isInstanceOf(BatchStreamException.class);
	}

	@Test
	public void specififyProcessingTimeWithoutBeingParallel() {
		Assertions
				.assertThatThrownBy(() -> BatchStream
						.of(Arrays.asList("1", "2", "3"))
						.maxProcessingTimeInSeconds(1))
				.isInstanceOf(IllegalArgumentException.class);
	}
}
