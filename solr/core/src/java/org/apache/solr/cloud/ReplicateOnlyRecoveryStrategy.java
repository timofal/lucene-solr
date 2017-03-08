/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicateOnlyRecoveryStrategy extends Thread implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int MAX_RETRIES = 500;
  private static final int STARTING_RECOVERY_DELAY = 5000;

  public static interface RecoveryListener {
    public void recovered();
    public void failed();
  }
  
  private volatile boolean close = false;

  private RecoveryStrategy.RecoveryListener recoveryListener;
  private ZkController zkController;
  private String baseUrl;
  private String coreZkNodeName;
  private ZkStateReader zkStateReader;
  private volatile String coreName;
  private int retries;
  private boolean recoveringAfterStartup;
  private CoreContainer cc;

  // this should only be used from SolrCoreState
  public ReplicateOnlyRecoveryStrategy(CoreContainer cc, CoreDescriptor cd, RecoveryStrategy.RecoveryListener recoveryListener) {
    this.cc = cc;
    this.coreName = cd.getName();
    this.recoveryListener = recoveryListener;
    setName("ReplicateOnlyRecoveryThread-"+this.coreName);
    zkController = cc.getZkController();
    zkStateReader = zkController.getZkStateReader();
    baseUrl = zkController.getBaseUrl();
    coreZkNodeName = cd.getCloudDescriptor().getCoreNodeName();
  }

  public void setRecoveringAfterStartup(boolean recoveringAfterStartup) {
    this.recoveringAfterStartup = recoveringAfterStartup;
  }

  // make sure any threads stop retrying
  @Override
  public void close() {
    close = true;
    LOG.warn("Stopping recovery for core=[{}] coreNodeName=[{}]", coreName, coreZkNodeName);
  }

  private void recoveryFailed(final SolrCore core,
      final ZkController zkController, final String baseUrl,
      final String shardZkNodeName, final CoreDescriptor cd) throws KeeperException, InterruptedException {
    SolrException.log(LOG, "Recovery failed - I give up.");
    try {
      zkController.publish(cd, Replica.State.RECOVERY_FAILED);
    } finally {
      close();
      recoveryListener.failed();
    }
  }
  
  private void replicate(String nodeName, SolrCore core, ZkNodeProps leaderprops)
      throws SolrServerException, IOException {

    ZkCoreNodeProps leaderCNodeProps = new ZkCoreNodeProps(leaderprops);
    String leaderUrl = leaderCNodeProps.getCoreUrl();
    
    LOG.info("Attempting to replicate from [{}].", leaderUrl);
    
    // send commit
    commitOnLeader(leaderUrl);
    
    // use rep handler directly, so we can do this sync rather than async
    SolrRequestHandler handler = core.getRequestHandler(ReplicationHandler.PATH);
    ReplicationHandler replicationHandler = (ReplicationHandler) handler;
    
    if (replicationHandler == null) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
          "Skipping recovery, no " + ReplicationHandler.PATH + " handler found");
    }
    
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(ReplicationHandler.MASTER_URL, leaderUrl);
    
    if (isClosed()) return; // we check closed on return
    boolean success = replicationHandler.doFetch(solrParams, false);
    
    if (!success) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Replication for recovery failed.");
    }
    
    // solrcloud_debug
    if (LOG.isDebugEnabled()) {
      try {
        RefCounted<SolrIndexSearcher> searchHolder = core
            .getNewestSearcher(false);
        SolrIndexSearcher searcher = searchHolder.get();
        Directory dir = core.getDirectoryFactory().get(core.getIndexDir(), DirContext.META_DATA, null);
        try {
          LOG.debug(core.getCoreDescriptor().getCoreContainer()
              .getZkController().getNodeName()
              + " replicated "
              + searcher.search(new MatchAllDocsQuery(), 1).totalHits
              + " from "
              + leaderUrl
              + " gen:"
              + (core.getDeletionPolicy().getLatestCommit() != null ? "null" : core.getDeletionPolicy().getLatestCommit().getGeneration())
              + " data:" + core.getDataDir()
              + " index:" + core.getIndexDir()
              + " newIndex:" + core.getNewIndexDir()
              + " files:" + Arrays.asList(dir.listAll()));
        } finally {
          core.getDirectoryFactory().release(dir);
          searchHolder.decref();
        }
      } catch (Exception e) {
        LOG.debug("Error in solrcloud_debug block", e);
      }
    }

  }

  private void commitOnLeader(String leaderUrl) throws SolrServerException,
      IOException {
    try (HttpSolrClient client = new HttpSolrClient.Builder(leaderUrl).build()) {
      client.setConnectionTimeout(30000);
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.getParams().set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
      ureq.getParams().set(UpdateParams.OPEN_SEARCHER, false);
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true).process(client);
    }
  }

  @Override
  public void run() {

    // set request info for logging
    try (SolrCore core = cc.getCore(coreName)) {

      if (core == null) {
        SolrException.log(LOG, "SolrCore not found - cannot recover:" + coreName);
        return;
      }
      MDCLoggingContext.setCore(core);

      try {
        doRecovery(core);
      } catch (InterruptedException e) {
        // nocommit: InterruptedException should't really happen now
        Thread.currentThread().interrupt();
        SolrException.log(LOG, "", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (Exception e) {
        LOG.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }
    } finally {
      MDCLoggingContext.clear();
    }
  }

  // TODO: perhaps make this grab a new core each time through the loop to handle core reloads?
  public void doRecovery(SolrCore core) throws KeeperException, InterruptedException {
    boolean successfulRecovery = false;

//    if (core.getUpdateHandler().getUpdateLog() != null) {
//      SolrException.log(LOG, "'replicate-only' recovery strategy should only be used if no update logs are present, but this core has one: "
//          + core.getUpdateHandler().getUpdateLog());
//      return;
//    }
    while (!successfulRecovery && !isInterrupted() && !isClosed()) { // don't use interruption or it will close channels though
      try {
        CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
        ZkNodeProps leaderprops = zkStateReader.getLeaderRetry(
            cloudDesc.getCollectionName(), cloudDesc.getShardId());
        final String leaderBaseUrl = leaderprops.getStr(ZkStateReader.BASE_URL_PROP);
        final String leaderCoreName = leaderprops.getStr(ZkStateReader.CORE_NAME_PROP);

        String leaderUrl = ZkCoreNodeProps.getCoreUrl(leaderBaseUrl, leaderCoreName);

        String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);

        boolean isLeader = leaderUrl.equals(ourUrl); //TODO: We can probably delete most of this code if we say this strategy can only be used for passive replicas
        if (isLeader && !cloudDesc.isLeader()) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Cloud state still says we are leader.");
        }
        if (cloudDesc.isLeader()) {
          // we are now the leader - no one else must have been suitable
          LOG.warn("We have not yet recovered - but we are now the leader!");
          LOG.info("Finished recovery process.");
          zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
          return;
        }
        
        
        LOG.info("Publishing state of core [{}] as recovering, leader is [{}] and I am [{}]", core.getName(), leaderUrl,
            ourUrl);
        zkController.publish(core.getCoreDescriptor(), Replica.State.RECOVERING);
        
        if (isClosed()) {
          LOG.info("Recovery for core {} has been closed", core.getName());
          break;
        }
        
        if (isClosed()) {
          LOG.info("Recovery for core {} has been closed", core.getName());
          break;
        }
        LOG.info("Starting Replication Recovery.");

        try {
          LOG.info("Stopping background replicate from leader process");
          zkController.stopReplicationFromLeader(coreName);
          replicate(zkController.getNodeName(), core, leaderprops);

          if (isClosed()) {
            LOG.info("Recovery for core {} has been closed", core.getName());
            break;
          }

          LOG.info("Replication Recovery was successful.");
          successfulRecovery = true;
        } catch (Exception e) {
          SolrException.log(LOG, "Error while trying to recover", e);
        }

      } catch (Exception e) {
        SolrException.log(LOG, "Error while trying to recover. core=" + coreName, e);
      } finally {
        if (successfulRecovery) {
          LOG.info("Restaring background replicate from leader process");
          zkController.startReplicationFromLeader(coreName, false);
          LOG.info("Registering as Active after recovery.");
          try {
            zkController.publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
          } catch (Exception e) {
            LOG.error("Could not publish as ACTIVE after succesful recovery", e);
            successfulRecovery = false;
          }
          
          if (successfulRecovery) {
            close = true;
            recoveryListener.recovered();
          }
        }
      }

      if (!successfulRecovery) {
        // lets pause for a moment and we need to try again...
        // TODO: we don't want to retry for some problems?
        // Or do a fall off retry...
        try {

          if (isClosed()) {
            LOG.info("Recovery for core {} has been closed", core.getName());
            break;
          }
          
          LOG.error("Recovery failed - trying again... (" + retries + ")");
          
          retries++;
          if (retries >= MAX_RETRIES) {
            SolrException.log(LOG, "Recovery failed - max retries exceeded (" + retries + ").");
            try {
              recoveryFailed(core, zkController, baseUrl, coreZkNodeName, core.getCoreDescriptor());
            } catch (Exception e) {
              SolrException.log(LOG, "Could not publish that recovery failed", e);
            }
            break;
          }
        } catch (Exception e) {
          SolrException.log(LOG, "An error has occurred during recovery", e);
        }

        try {
          // Wait an exponential interval between retries, start at 5 seconds and work up to a minute.
          // If we're at attempt >= 4, there's no point computing pow(2, retries) because the result 
          // will always be the minimum of the two (12). Since we sleep at 5 seconds sub-intervals in
          // order to check if we were closed, 12 is chosen as the maximum loopCount (5s * 12 = 1m).
          double loopCount = retries < 4 ? Math.min(Math.pow(2, retries), 12) : 12;
          LOG.info("Wait [{}] seconds before trying to recover again (attempt={})", loopCount, retries);
          for (int i = 0; i < loopCount; i++) {
            if (isClosed()) {
              LOG.info("Recovery for core {} has been closed", core.getName());
              break; // check if someone closed us
            }
            Thread.sleep(STARTING_RECOVERY_DELAY);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Recovery was interrupted.", e);
          close = true;
        }
      }

    }

    // if replay was skipped (possibly to due pulling a full index from the leader),
    // then we still need to update version bucket seeds after recovery
    if (successfulRecovery) {
      LOG.info("Updating version bucket highest from index after successful recovery.");
      core.seedVersionBuckets();
    }

    LOG.info("Finished recovery process, successful=[{}]", Boolean.toString(successfulRecovery));
  }

  public boolean isClosed() {
    return close;
  }

}
