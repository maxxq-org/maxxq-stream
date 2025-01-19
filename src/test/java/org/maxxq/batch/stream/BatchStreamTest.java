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
	@RepeatedTest(100)
	public void map() {
		List<Integer> result = BatchStream
				.of(Arrays.asList("1", "2", "3"))
				.parallel(Executors.newFixedThreadPool(3))
//				.maxProcessingTimeInSeconds(10)
				.map(numericString -> numericString + "0")
				.map(someString -> Integer.parseInt(someString))
				.consume(number -> sleep(number))
				.collect(new ArrayList<Integer>());
		
		result.forEach(number -> System.out.println(number));

		//because of the parallel processing and sleep time, results will endup in the opposite order
		//this proves parallel processing really took place and the resulting collection can have a different
		//order than the input collection
		Assertions.assertThat(result.size()).isEqualTo(3);
		Assertions.assertThat(result.get(0)).isEqualTo(30);
		Assertions.assertThat(result.get(1)).isEqualTo(20);
		Assertions.assertThat(result.get(2)).isEqualTo(10);
	}

	private void sleep(int number) {
		try {
			Thread.sleep(200 -  2 * number);
		} catch (InterruptedException e) {
		}
	}
}
