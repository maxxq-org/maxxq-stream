package org.maxxq.batch.stream;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class BatchStreamTest {
	private final Random random = new Random();

	@Test
	public void map() {
		Collection<Integer> result = BatchStream
				.of(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"))
				.parallel(Executors.newFixedThreadPool(10))
				.timeoutForSuppliersToRespond(10)
				.consume(numericString -> System.out.println("1: " + numericString))
				.map(numericString -> numericString + "0")
				.consume(numericString -> System.out.println("2: " + numericString))
				.map(someString -> Integer.parseInt(someString))
				.consume(number -> System.out.println("3: " + number))
				.consume(number -> sleep(number))
				.consume(number -> System.out.println("4: " + number))
				.collect();
		
		result.forEach(number -> System.out.println(number));

		Assertions.assertThat(result.size()).isEqualTo(10);
		Assertions.assertThat(result).contains(10);
		Assertions.assertThat(result).contains(20);
	}

	private void sleep(int number) {
		try {
			Thread.sleep(Math.abs(random.nextInt() % 10000));
//			Thread.sleep(1000 - 10 * number);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
