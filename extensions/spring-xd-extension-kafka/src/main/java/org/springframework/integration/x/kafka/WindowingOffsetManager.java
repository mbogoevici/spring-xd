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

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.observables.GroupedObservable;
import rx.observables.MathObservable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.integration.kafka.core.Partition;
import org.springframework.integration.kafka.listener.OffsetManager;

/**
 * @author Marius Bogoevici
 */
public class WindowingOffsetManager implements OffsetManager, InitializingBean {

	private OffsetManager delegate;

	private int timespan = 10;

	private Subject<PartitionAndOffset, PartitionAndOffset> offsets;

	public WindowingOffsetManager(OffsetManager offsetManager) {
		this.delegate = offsetManager;
	}

	public void setTimespan(int timespan) {
		this.timespan = timespan;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		offsets = new SerializedSubject(PublishSubject.<PartitionAndOffset>create());
		offsets
				.window(10, TimeUnit.SECONDS)
				.flatMap(new Func1<Observable<PartitionAndOffset>, Observable<PartitionAndOffset>>() {
					@Override
					public Observable<PartitionAndOffset> call(Observable<PartitionAndOffset> windowBuffer) {
						return windowBuffer.groupBy(new Func1<PartitionAndOffset, Partition>() {
							@Override
							public Partition call(PartitionAndOffset partitionAndOffset) {
								return partitionAndOffset.getPartition();
							}
						}).flatMap(new Func1<GroupedObservable<Partition, PartitionAndOffset>, Observable<PartitionAndOffset>>() {
							@Override
							public Observable<PartitionAndOffset> call(GroupedObservable<Partition, PartitionAndOffset> group) {
								return Observable.zip(
										Observable.just(group.getKey()),
										MathObservable.max(group.map(new Func1<PartitionAndOffset, Long>() {
											@Override
											public Long call(PartitionAndOffset partitionAndOffset) {
												return partitionAndOffset.getOffset();
											}
										})), new Func2<Partition, Long, PartitionAndOffset>() {
											@Override
											public PartitionAndOffset call(Partition partition, Long offset) {
												return new PartitionAndOffset(partition, offset);
											}
										}
								);
							}
						});
					}
				}).subscribe(new Action1<PartitionAndOffset>() {
			@Override
			public void call(PartitionAndOffset partitionAndOffset) {
				delegate.updateOffset(partitionAndOffset.getPartition(), partitionAndOffset.getOffset());
			}
		});
	}

	@Override
	public void updateOffset(Partition partition, long offset) {
		offsets.onNext(new PartitionAndOffset(partition, offset));
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

	class PartitionAndOffset {

		private Partition partition;

		private Long offset;

		public PartitionAndOffset(Partition partition, Long offset) {
			this.partition = partition;
			this.offset = offset;
		}

		public Partition getPartition() {
			return partition;
		}

		public Long getOffset() {
			return offset;
		}
	}
}
