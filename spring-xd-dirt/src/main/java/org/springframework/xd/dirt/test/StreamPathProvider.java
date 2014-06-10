/*
 * Copyright 2013-2014 the original author or authors.
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

package org.springframework.xd.dirt.test;

import java.util.Map;

import org.apache.curator.framework.CuratorFramework;

import org.springframework.xd.dirt.core.Stream;
import org.springframework.xd.dirt.module.ModuleDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamDefinitionRepository;
import org.springframework.xd.dirt.stream.StreamFactory;
import org.springframework.xd.dirt.util.MapBytesUtility;
import org.springframework.xd.dirt.zookeeper.Paths;
import org.springframework.xd.dirt.zookeeper.ZooKeeperConnection;
import org.springframework.xd.dirt.zookeeper.ZooKeeperUtils;
import org.springframework.xd.module.options.ModuleOptionsMetadataResolver;

/**
 * Provides path information for stream definitions, deployments, and stream module deployments.
 *
 * @author David Turanski
 * @author Mark Fisher
 * @author Patrick Peralta
 */
public class StreamPathProvider implements DeploymentPathProvider {

	/**
	 * ZooKeeper connection.
	 */
	private final ZooKeeperConnection zkConnection;

	/**
	 * Stream factory.
	 */
	private final StreamFactory streamFactory;

	/**
	 * Utility to convert JSON strings to maps.
	 */
	private final MapBytesUtility mapBytesUtility = new MapBytesUtility();

	/**
	 * Construct a StreamPathProvider.
	 *
	 * @param zkConnection ZooKeeper connection
	 * @param streamDefinitionRepository repository for stream definitions
	 * @param moduleDefinitionRepository repository for module definitions
	 * @param moduleOptionsMetadataResolver resolver for module options metadata
	 */
	public StreamPathProvider(ZooKeeperConnection zkConnection,
			StreamDefinitionRepository streamDefinitionRepository,
			ModuleDefinitionRepository moduleDefinitionRepository,
			ModuleOptionsMetadataResolver moduleOptionsMetadataResolver) {
		this.zkConnection = zkConnection;
		this.streamFactory = new StreamFactory(streamDefinitionRepository, moduleDefinitionRepository,
				moduleOptionsMetadataResolver);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDefinitionPath(String streamName) {
		return Paths.build(Paths.STREAMS, streamName);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDeploymentPath(String streamName) {
		return Paths.build(Paths.STREAM_DEPLOYMENTS, streamName, Paths.MODULES);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getDeploymentPathChildrenCount(String streamName) {
		try {
			Stream stream = streamFactory.createStream(streamName, getStreamProperties(streamName));
			return stream.getModuleDescriptors().size();
		}
		catch (Exception e) {
			String definition;
			try {
				byte[] data = zkConnection.getClient().getData().forPath(getDefinitionPath(streamName));
				definition = new String(data);
			}
			catch (Exception ex) {
				definition = "Could not load definition due to: " + ex.toString();
			}
			throw new IllegalStateException(String.format(
					"Failed to determine module deployment paths for stream %s, definition: %s",
					streamName, definition), e);
		}
	}

	/**
	 * Return the data for a stream from ZooKeeper.
	 *
	 * @param streamName stream name
	 * @return properties for a stream
	 */
	private Map<String, String> getStreamProperties(String streamName) {
		CuratorFramework client = zkConnection.getClient();

		try {
			byte[] data = client.getData().forPath(Paths.build(Paths.STREAMS, streamName));
			return mapBytesUtility.toMap(data);
		}
		catch (Exception e) {
			throw ZooKeeperUtils.wrapThrowable(e);
		}
	}

}
