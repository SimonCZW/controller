/*
 * Copyright (c) 2014 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.controller.cluster.datastore;

import akka.actor.ActorSystem;
import org.opendaylight.controller.cluster.ActorSystemProvider;
import org.opendaylight.controller.cluster.databroker.ClientBackedDataStore;
import org.opendaylight.controller.cluster.datastore.config.Configuration;
import org.opendaylight.controller.cluster.datastore.config.ConfigurationImpl;
import org.opendaylight.controller.cluster.datastore.persisted.DatastoreSnapshot;
import org.opendaylight.mdsal.dom.api.DOMSchemaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DistributedDataStoreFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedDataStoreFactory.class);
    private static final String DEFAULT_MODULE_SHARDS_PATH = "./configuration/initial/module-shards.conf";
    private static final String DEFAULT_MODULES_PATH = "./configuration/initial/modules.conf";

    private DistributedDataStoreFactory() {
    }

    /**
     * czw增加注释:
     * @param schemaService org.opendaylight.mdsal.dom.api.DOMSchemaService
     * @param initialDatastoreContext 封装了config datastore的配置(context), org.opendaylight.controller.cluster.datastore.DatastoreContext
     * @param datastoreSnapshotRestore 读取backup datastore, org.opendaylight.controller.cluster.datastore.DatastoreSnapshotRestore
     * @param actorSystemProvider 提供akka actorSystem, org.opendaylight.controller.config.yang.config.actor_system_provider.impl.ActorSystemProviderImpl
     * @param introspector   org.opendaylight.controller.cluster.datastore.DatastoreContextIntrospector
     * @param updater org.opendaylight.controller.cluster.datastore.DatastoreContextPropertiesUpdater
     * @return
     */
    public static AbstractDataStore createInstance(final DOMSchemaService schemaService,
            final DatastoreContext initialDatastoreContext, final DatastoreSnapshotRestore datastoreSnapshotRestore,
            final ActorSystemProvider actorSystemProvider, final DatastoreContextIntrospector introspector,
            final DatastoreContextPropertiesUpdater updater) {
        return createInstance(schemaService, initialDatastoreContext, datastoreSnapshotRestore, actorSystemProvider,
                introspector, updater, null);
    }

    public static AbstractDataStore createInstance(final DOMSchemaService schemaService,
            final DatastoreContext initialDatastoreContext, final DatastoreSnapshotRestore datastoreSnapshotRestore,
            final ActorSystemProvider actorSystemProvider, final DatastoreContextIntrospector introspector,
            final DatastoreContextPropertiesUpdater updater, final Configuration orgConfig) {

        // datastore name: config
        final String datastoreName = initialDatastoreContext.getDataStoreName();
        LOG.info("Create data store instance of type : {}", datastoreName);

        final ActorSystem actorSystem = actorSystemProvider.getActorSystem();
        final DatastoreSnapshot restoreFromSnapshot = datastoreSnapshotRestore.getAndRemove(datastoreName);

        Configuration config;
        if (orgConfig == null) {
            // 读取modules/module-shards配置
            //  DEFAULT_MODULE_SHARDS_PATH = "./configuration/initial/module-shards.conf";
            //  DEFAULT_MODULES_PATH = "./configuration/initial/modules.conf";
            config = new ConfigurationImpl(DEFAULT_MODULE_SHARDS_PATH, DEFAULT_MODULES_PATH);
        } else {
            config = orgConfig;
        }
        // actorSystem含有akka配置, 这里封装了akka cluster, 完成akka cluster集群协商
        final ClusterWrapper clusterWrapper = new ClusterWrapperImpl(actorSystem);

        final DatastoreContextFactory contextFactory = introspector.newContextFactory();

        // This is the potentially-updated datastore context, distinct from the initial one
        final DatastoreContext datastoreContext = contextFactory.getBaseDatastoreContext();

        final AbstractDataStore dataStore;
        /*
           根据设置创建datastore,创建datastore均会:
            1.创建shard manager actor
            2.创建本地datastore client actor
            3.创建actorContext(有shard manager actor的ActorRef/可以发消息给manager)
         */
        if (datastoreContext.isUseTellBasedProtocol()) {
            dataStore = new ClientBackedDataStore(actorSystem, clusterWrapper, config, contextFactory,
                restoreFromSnapshot);
            LOG.info("Data store {} is using tell-based protocol", datastoreName);
        } else { // YANGL distributed-datastore-provider中默认值为false
            // 在DistributedConfigDataStoreProviderModule设置了isUseTellBasedProtocol为false --> 理解配置了cluster模式,后台创建的dataStore在这里被创建为分布式
            // 创建DistributedDataStore
            dataStore = new DistributedDataStore(actorSystem, clusterWrapper, config, contextFactory,
                restoreFromSnapshot);
            LOG.info("Data store {} is using ask-based protocol", datastoreName);
        }
        // 监听dataStore在运行期间context变化
        updater.setListener(dataStore);

        schemaService.registerSchemaContextListener(dataStore);

        dataStore.setCloseable(updater);
        dataStore.waitTillReady();

        return dataStore;
    }
}
