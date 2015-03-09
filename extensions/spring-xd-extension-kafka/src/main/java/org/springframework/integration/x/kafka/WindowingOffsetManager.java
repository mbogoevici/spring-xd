/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.integration.x.kafka;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.rx.BiStreams;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.listener.OffsetManager;

/**
 * @author Marius Bogoevici
 */
public class WindowingOffsetManager implements OffsetManager, InitializingBean {

	private final Environment environment;

	private OffsetManager delegate;

	private Broadcaster<Tuple2<Partition, Long>> offsets;

	private int windowSize = 100;

	private int timespan = 10;

	public WindowingOffsetManager(OffsetManager offsetManager) {
		environment = Environment.initializeIfEmpty();
		this.delegate = offsetManager;
	}

	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}

	public void setTimespan(int timespan) {
		this.timespan = timespan;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		offsets = Broadcaster.create(environment);
		offsets
				.window(windowSize, timespan, TimeUnit.SECONDS).flatMap(new Function<Stream<Tuple2<Partition, Long>>, Publisher<Tuple2<Partition, Long>>>() {
			@Override
			public Publisher<Tuple2<Partition, Long>> apply(Stream<Tuple2<Partition, Long>> tuple2Stream) {
				return BiStreams.reduceByKey(tuple2Stream, new BiFunction<Long, Long, Long>() {
					@Override
					public Long apply(Long aLong, Long aLong2) {
						return Math.max(aLong, aLong2);
					}
				});
			}
		})
				.consume(new Consumer<Tuple2<Partition, Long>>() {
					@Override
					public void accept(Tuple2<Partition, Long> partitionLongTuple2) {
						delegate.updateOffset(partitionLongTuple2.getT1(), partitionLongTuple2.getT2());
					}
				});
	}

	@Override
	public void updateOffset(Partition partition, long offset) {
		offsets.onNext(Tuple.of(partition, offset));
	}

	@Override
	public long getOffset(Partition partition) {
		return delegate.getOffset(partition);
	}

	@Override
	public void deleteOffset(Partition partition) {
		delegate.deleteOffset(partition);
	}

	@Override
	public void resetOffsets(Collection<Partition> partition) {
		delegate.resetOffsets(partition);
	}

	@Override
	public void close() throws IOException {
		delegate.close();
	}

	@Override
	public void flush() throws IOException {
		delegate.flush();
	}
}
