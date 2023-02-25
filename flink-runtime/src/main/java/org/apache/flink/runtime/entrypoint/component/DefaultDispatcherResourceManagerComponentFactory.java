/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherOperationCaches;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobPersistenceComponentFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent}
 * components.
 */
public class DefaultDispatcherResourceManagerComponentFactory
        implements DispatcherResourceManagerComponentFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nonnull
    private final DispatcherRunnerFactory dispatcherRunnerFactory;

    @Nonnull
    private final ResourceManagerFactory<?> resourceManagerFactory;

    @Nonnull
    private final RestEndpointFactory<?> restEndpointFactory;

    public DefaultDispatcherResourceManagerComponentFactory(
            @Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
            @Nonnull ResourceManagerFactory<?> resourceManagerFactory,
            @Nonnull RestEndpointFactory<?> restEndpointFactory) {
        this.dispatcherRunnerFactory = dispatcherRunnerFactory;
        this.resourceManagerFactory = resourceManagerFactory;
        this.restEndpointFactory = restEndpointFactory;
    }

    @Override
    public DispatcherResourceManagerComponent create(
            Configuration configuration,
            ResourceID resourceId,
            Executor ioExecutor,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            BlobServer blobServer,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            MetricRegistry metricRegistry,
            ExecutionGraphInfoStore executionGraphInfoStore,
            MetricQueryServiceRetriever metricQueryServiceRetriever,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        LeaderRetrievalService dispatcherLeaderRetrievalService = null;
        LeaderRetrievalService resourceManagerRetrievalService = null;
        WebMonitorEndpoint<?> webMonitorEndpoint = null;
        ResourceManagerService resourceManagerService = null;
        DispatcherRunner dispatcherRunner = null;

        try {
            dispatcherLeaderRetrievalService =
                    highAvailabilityServices.getDispatcherLeaderRetriever();

            resourceManagerRetrievalService =
                    highAvailabilityServices.getResourceManagerLeaderRetriever();

            final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            DispatcherGateway.class,
                            DispatcherId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            ResourceManagerGateway.class,
                            ResourceManagerId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            final ScheduledExecutorService executor =
                    WebMonitorEndpoint.createExecutorService(
                            configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
                            configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
                            "DispatcherRestEndpoint");

            final long updateInterval =
                    configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
            final MetricFetcher metricFetcher =
                    updateInterval == 0
                            ? VoidMetricFetcher.INSTANCE
                            : MetricFetcherImpl.fromConfiguration(
                            configuration,
                            metricQueryServiceRetriever,
                            dispatcherGatewayRetriever,
                            executor);
            /**
             *  使用工厂类创建 WebMonitorEndpoint 实例， 在 Standalone模式下：DispatcherRestEndpoint
             *  1、restEndpointFactory = SessionRestEndpointFactory
             *  2、webMonitorEndpoint = DispatcherRestEndpoint
             *  3、highAvailabilityServices.getClusterRestEndpointLeaderElectionService() = ZooKeeperLeaderElectionService
             */
            webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
                    configuration,
                    dispatcherGatewayRetriever,
                    resourceManagerGatewayRetriever,
                    blobServer,
                    executor,
                    metricFetcher,
                    highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
                    fatalErrorHandler);

            log.debug("Starting Dispatcher REST endpoint.");
            /**
             * 启动 webMonitorEndpoint，即启动 DispatcherRestEndpoint
             * 1. 初始化 handler，启动 Netty 客户端
             * 2. 选举，选举成功后触发 grantLeadership 确认信息
             * 3. 启动定时任务 ExecutionGraphCacheCleanupTask
             */
            webMonitorEndpoint.start();

            final String hostname = RpcUtils.getHostname(rpcService);

            final HistoryServerArchivist historyServerArchivist =
                    HistoryServerArchivist.createHistoryServerArchivist(
                            configuration, webMonitorEndpoint, ioExecutor);

            final DispatcherOperationCaches dispatcherOperationCaches =
                    new DispatcherOperationCaches(
                            configuration.get(RestOptions.ASYNC_OPERATION_STORE_DURATION));

            final PartialDispatcherServices partialDispatcherServices =
                    new PartialDispatcherServices(
                            configuration,
                            highAvailabilityServices,
                            resourceManagerGatewayRetriever,
                            blobServer,
                            heartbeatServices,
                            () ->
                                    JobManagerMetricGroup.createJobManagerMetricGroup(
                                            metricRegistry, hostname),
                            executionGraphInfoStore,
                            fatalErrorHandler,
                            historyServerArchivist,
                            metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
                            ioExecutor,
                            dispatcherOperationCaches);

            log.debug("Starting Dispatcher.");
            /**
             * 创建 dispatcherRunner，内部创建和启动 dispatcher
             * 1. 创建 DispatcherRunnerLeaderElectionLifecycleManager
             * 2. dispatcher 选举
             * 3. 选举成功后调用 DefaultMultipleComponentLeaderElectionService isLeader
             * 4. isLeader 调用 resourceManagerServiceImpl grantLeadership 最终触发 DefaultDispatcherRunner grantLeadership
             * 5. 创建 DispatcherLeaderProcess，然后调用 DispatcherLeaderProcess start 启动
             * 6. 调用 SessionDispatcherLeaderProcess onStart
             * 7. 创建 StandaloneDispatcher ，最终调用 dispatcher.start 启动
             * 5. dispatcher.start 调用父类 RpcEndpoint.start，发送akka启动消息
             * 6. dispatcher 接收 启动消息，执行 Dispatcher onStart 方法启动
             */
            dispatcherRunner =
                    dispatcherRunnerFactory.createDispatcherRunner(
                            highAvailabilityServices.getDispatcherLeaderElectionService(),
                            fatalErrorHandler,
                            new HaServicesJobPersistenceComponentFactory(highAvailabilityServices),
                            ioExecutor,
                            rpcService,
                            partialDispatcherServices);

            // 创建 resourceManagerService
            resourceManagerService =
                    ResourceManagerServiceImpl.create(
                            resourceManagerFactory,
                            configuration,
                            resourceId,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            delegationTokenManager,
                            fatalErrorHandler,
                            new ClusterInformation(hostname, blobServer.getPort()),
                            webMonitorEndpoint.getRestBaseUrl(),
                            metricRegistry,
                            hostname,
                            ioExecutor);
            log.debug("Starting ResourceManagerService.");
            /**
             * 启动 resourceManagerService
             * 1. resourceManager 选举
             * 2. 选举成功后调用 DefaultMultipleComponentLeaderElectionService isLeader
             * 3. isLeader 调用 resourceManagerServiceImpl grantLeadership 最终触发 new resourceManager()，最终 调用父类 RpcEndpoint 启动 RPC 服务器
             * 4. resourceManagerServiceImpl 创建完 resourceManager 后，最终调用 resourceManager.start
             * 5. resourceManager.start 调用父类 RpcEndpoint.start，发送akka启动消息
             * 6. resourceManager 接收 启动消息，执行 resourceManager onStart 方法启动
             */
            resourceManagerService.start();

            resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
            dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

            return new DispatcherResourceManagerComponent(
                    dispatcherRunner,
                    resourceManagerService,
                    dispatcherLeaderRetrievalService,
                    resourceManagerRetrievalService,
                    webMonitorEndpoint,
                    fatalErrorHandler,
                    dispatcherOperationCaches);

        } catch (Exception exception) {
            // clean up all started components
            if (dispatcherLeaderRetrievalService != null) {
                try {
                    dispatcherLeaderRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (resourceManagerRetrievalService != null) {
                try {
                    resourceManagerRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (webMonitorEndpoint != null) {
                terminationFutures.add(webMonitorEndpoint.closeAsync());
            }

            if (resourceManagerService != null) {
                terminationFutures.add(resourceManagerService.closeAsync());
            }

            if (dispatcherRunner != null) {
                terminationFutures.add(dispatcherRunner.closeAsync());
            }

            final FutureUtils.ConjunctFuture<Void> terminationFuture =
                    FutureUtils.completeAll(terminationFutures);

            try {
                terminationFuture.get();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            throw new FlinkException(
                    "Could not create the DispatcherResourceManagerComponent.", exception);
        }
    }

    public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        SessionDispatcherFactory.INSTANCE),
                resourceManagerFactory,
                SessionRestEndpointFactory.INSTANCE);
    }

    public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory, JobGraphRetriever jobGraphRetriever) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),
                resourceManagerFactory,
                JobRestEndpointFactory.INSTANCE);
    }
}
