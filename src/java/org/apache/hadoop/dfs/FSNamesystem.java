/**
 * Copyright 2005 The Apache Software Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.dfs;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.logging.*;


// zeng: 管理file block datanode相关信息

/***************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 *
 * It tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
class FSNamesystem implements FSConstants {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.fs.FSNamesystem");

    //
    // Stores the correct file name hierarchy
    //
    FSDirectory dir;

    // zeng: block -> datanode set 映射
    //
    // Stores the block-->datanode(s) map.  Updated only in response
    // to client-sent information.
    //
    TreeMap blocksMap = new TreeMap();

    // zeng: datanode名称 -> DatanodeInfo对象 映射
    // Stores the datanode-->block map.  Done by storing a 
    // set of datanode info objects, sorted by name.  Updated only in
    // response to client-sent information.
    //
    TreeMap datanodeMap = new TreeMap();

    // zeng: datanode名称 -> 要删除的block副本set
    //
    // Keeps a Vector for every named machine.  The Vector contains
    // blocks that have recently been invalidated and are thought to live
    // on the machine in question.
    //
    TreeMap recentInvalidateSets = new TreeMap();

    // zeng: datanode名称 -> 多余block副本 set
    //
    // Keeps a TreeSet for every named node.  Each treeset contains
    // a list of the blocks that are "extra" at that location.  We'll
    // eventually remove these extras.
    //
    TreeMap excessReplicateMap = new TreeMap();

    // zeng: 创建中的文件 文件全描述符 -> block set 映射
    //
    // Keeps track of files that are being created, plus the
    // blocks that make them up.
    //
    TreeMap pendingCreates = new TreeMap();

    // zeng: 所有 正在创建中的文件的block
    //
    // Keeps track of the blocks that are part of those pending creates
    //
    TreeSet pendingCreateBlocks = new TreeSet();

    //
    // Stats on overall usage
    //
    long totalCapacity = 0, totalRemaining = 0;

    //
    Random r = new Random();

    // zeng: 心跳的所有节点
    //
    // Stores a set of datanode info objects, sorted by heartbeat
    //
    TreeSet heartbeats = new TreeSet(new Comparator() {
        public int compare(Object o1, Object o2) {
            DatanodeInfo d1 = (DatanodeInfo) o1;
            DatanodeInfo d2 = (DatanodeInfo) o2;
            long lu1 = d1.lastUpdate();
            long lu2 = d2.lastUpdate();
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return d1.getName().compareTo(d2.getName());
            }
        }
    });

    //
    // Store set of Blocks that need to be replicated 1 or more times.
    // We also store pending replication-orders.
    //
    // zeng: 需要复制的block
    private TreeSet neededReplications = new TreeSet();
    // zeng: 正在复制中的block (用来在复制失败时重新发起复制任务, 这个版本还没有实现重新发起的逻辑)
    private TreeSet pendingReplications = new TreeSet();

    //
    // Used for handling lock-leases
    //
    private TreeMap leases = new TreeMap();
    private TreeSet sortedLeases = new TreeSet();

    //
    // Threaded object that checks to see if we have been
    // getting heartbeats from all clients. 
    //
    HeartbeatMonitor hbmon = null;
    LeaseMonitor lmon = null;
    Daemon hbthread = null, lmthread = null;
    boolean fsRunning = true;
    long systemStart = 0;
    private Configuration conf;

    //  DESIRED_REPLICATION is how many copies we try to have at all times
    private int desiredReplication;
    //  The maximum number of replicates we should allow for a single block
    private int maxReplication;
    //  How many outgoing replication streams a given node should have at one time
    private int maxReplicationStreams;
    // MIN_REPLICATION is how many copies we need in place or else we disallow the write
    private int minReplication;
    // HEARTBEAT_RECHECK is how often a datanode sends its hearbeat
    private int heartBeatRecheck;

    /**
     * dir is where the filesystem directory state
     * is stored
     */
    public FSNamesystem(File dir, Configuration conf) throws IOException {
        // zeng: 文件树表示
        this.dir = new FSDirectory(dir);

        // zeng: 心跳检测线程
        this.hbthread = new Daemon(new HeartbeatMonitor());
        // zeng: 租约检测线程
        this.lmthread = new Daemon(new LeaseMonitor());
        hbthread.start();
        lmthread.start();

        this.systemStart = System.currentTimeMillis();
        this.conf = conf;

        // zeng: 每个block有几个副本
        this.desiredReplication = conf.getInt("dfs.replication", 3);
        this.maxReplication = desiredReplication;

        // zeng: 一个datanode同时最多处理多少个复制任务
        this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);

        // zeng: block最少副本
        this.minReplication = 1;

        // zeng: 多久检测一次心跳
        this.heartBeatRecheck = 1000;
    }

    /**
     * Close down this filesystem manager.
     * Causes heartbeat and lease daemons to stop; waits briefly for
     * them to finish, but a short timeout returns control back to caller.
     */
    public void close() {
        synchronized (this) {
            fsRunning = false;
        }

        // zeng: fsRunning为false后, 心跳检测线程会退出无限循环,这里等待3s让它们能完全退出
        try {
            hbthread.join(3000);
        } catch (InterruptedException ie) {
        } finally {
            // using finally to ensure we also wait for lease daemon
            try {
                lmthread.join(3000);
            } catch (InterruptedException ie) {
            }
        }
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by HadoopFS clients
    //
    /////////////////////////////////////////////////////////

    /**
     * The client wants to open the given filename.  Return a
     * list of (block,machineArray) pairs.  The sequence of unique blocks
     * in the list indicates all the blocks that make up the filename.
     * <p>
     * The client should choose one of the machines from the machineArray
     * at random.
     */
    public Object[] open(UTF8 src) {
        Object results[] = null;
        // zeng: 获取文件包含的block
        Block blocks[] = dir.getFile(src);

        if (blocks != null) {
            results = new Object[2];

            DatanodeInfo machineSets[][] = new DatanodeInfo[blocks.length][];

            // zeng: 这些block在哪些datanode下
            for (int i = 0; i < blocks.length; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);

                if (containingNodes == null) {
                    machineSets[i] = new DatanodeInfo[0];
                } else {
                    machineSets[i] = new DatanodeInfo[containingNodes.size()];

                    // zeng: 哪些datanode下
                    int j = 0;
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); j++) {
                        machineSets[i][j] = (DatanodeInfo) it.next();
                    }
                }
            }

            // zeng: 哪些block
            results[0] = blocks;
            // zeng: 这些block在哪些datanode下
            results[1] = machineSets;
        }

        return results;
    }

    // zeng: 为新文件 分配第一个block 并为 block选择DatanodeInfo

    /**
     * The client would like to create a new block for the indicated
     * filename.  Return an array that consists of the block, plus a set
     * of machines.  The first on this list should be where the client
     * writes data.  Subsequent items in the list must be provided in
     * the connection to the first datanode.
     *
     * @return Return an array that consists of the block, plus a set
     * of machines, or null if src is invalid for creation (based on
     * {@link FSDirectory#isValidToCreate(UTF8)}.
     */
    public synchronized Object[] startFile(UTF8 src, UTF8 holder, UTF8 clientMachine, boolean overwrite) {
        Object results[] = null;
        if (pendingCreates.get(src) == null) {  // zeng: 如果在pendingCreates里, 表示这个file在创建流程中
            boolean fileValid = dir.isValidToCreate(src);

            // zeng; 文件已存在就覆盖
            if (overwrite && !fileValid) {
                // zeng: 从文件树中删除
                delete(src);

                fileValid = true;
            }

            if (fileValid) {
                results = new Object[2];

                // Get the array of replication targets
                // zeng: 给block选择DataNode, 返回DatanodeInfo数组
                DatanodeInfo targets[] = chooseTargets(this.desiredReplication, null, clientMachine);

                if (targets.length < this.minReplication) {
                    LOG.warning("Target-length is " + targets.length +
                            ", below MIN_REPLICATION (" + this.minReplication + ")");

                    return null;
                }

                // zeng: pendingCreates 该文件对应的vector
                // Reserve space for this pending file
                pendingCreates.put(src, new Vector());

                // zeng: TODO 租约 没看到校验租约的地方 0.1.0未实现完?
                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);

                    if (lease == null) {
                        lease = new Lease(holder);
                        leases.put(holder, lease);
                        sortedLeases.add(lease);
                    } else {
                        sortedLeases.remove(lease);
                        lease.renew();
                        sortedLeases.add(lease);
                    }

                    lease.startedCreate(src);
                }

                // zeng: 分配该file下的一个block对象
                // Create next block
                results[0] = allocateBlock(src);

                // zeng: 选择的DatanodeInfo
                results[1] = targets;
            } else { // ! fileValid
                LOG.warning("Cannot start file because it is invalid. src=" + src);
            }
        } else {
            LOG.warning("Cannot start file because pendingCreates is non-null. src=" + src);
        }

        return results;
    }

    // zeng: 为文件增加一个block 并为block选择DatanodeInfo

    /**
     * The client would like to obtain an additional block for the indicated
     * filename (which is being written-to).  Return an array that consists
     * of the block, plus a set of machines.  The first on this list should
     * be where the client writes data.  Subsequent items in the list must
     * be provided in the connection to the first datanode.
     * <p>
     * Make sure the previous blocks have been reported by datanodes and
     * are replicated.  Will return an empty 2-elt array if we want the
     * client to "try again later".
     */
    public synchronized Object[] getAdditionalBlock(UTF8 src, UTF8 clientMachine) {
        Object results[] = null;
        if (dir.getFile(src) == null && pendingCreates.get(src) != null) {
            results = new Object[2];

            //
            // If we fail this, bad things happen!
            //
            if (checkFileProgress(src)) {   // zeng: 文件已经已经有其他block 并且 其他的block都有足够的副本
                // Get the array of replication targets
                // zeng: 为block分配datanode
                DatanodeInfo targets[] = chooseTargets(this.desiredReplication, null, clientMachine);

                if (targets.length < this.minReplication) {
                    return null;
                }

                // Create next block
                //zeng: 分配该file下的一个block对象
                results[0] = allocateBlock(src);

                results[1] = targets;
            }
        }

        return results;
    }

    /**
     * The client would like to let go of the given block
     */
    public synchronized boolean abandonBlock(Block b, UTF8 src) {
        //
        // Remove the block from the pending creates list
        //
        Vector pendingVector = (Vector) pendingCreates.get(src);
        if (pendingVector != null) {
            for (Iterator it = pendingVector.iterator(); it.hasNext(); ) {
                Block cur = (Block) it.next();
                if (cur.compareTo(b) == 0) {
                    pendingCreateBlocks.remove(cur);
                    it.remove();
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Abandon the entire file in progress
     */
    public synchronized void abandonFileInProgress(UTF8 src) throws IOException {
        internalReleaseCreate(src);
    }

    // zeng: 设置block的len, 将文件加入文件树

    /**
     * Finalize the created file and make it world-accessible.  The
     * FSNamesystem will already know the blocks that make up the file.
     * Before we return, we make sure that all the file's blocks have
     * been reported by datanodes and are replicated correctly.
     */
    public synchronized int completeFile(UTF8 src, UTF8 holder) {
        if (dir.getFile(src) != null || pendingCreates.get(src) == null) {
            LOG.info("Failed to complete " + src + "  because dir.getFile()==" + dir.getFile(src) + " and " + pendingCreates.get(src));
            return OPERATION_FAILED;
        } else if (!checkFileProgress(src)) {   // zeng:  如果block没有足够的副本
            return STILL_WAITING;
        } else {
            Vector pendingVector = (Vector) pendingCreates.get(src);
            Block pendingBlocks[] = (Block[]) pendingVector.toArray(new Block[pendingVector.size()]);

            // zeng: 遍历这个文件下已上传完毕的所有block, 设置DatanodeInfo.blocks中的block对象的len
            //
            // We have the pending blocks, but they won't have
            // length info in them (as they were allocated before
            // data-write took place).  So we need to add the correct
            // length info to each
            //
            // REMIND - mjc - this is very inefficient!  We should
            // improve this!
            //
            for (int i = 0; i < pendingBlocks.length; i++) {
                Block b = pendingBlocks[i];
                TreeSet containingNodes = (TreeSet) blocksMap.get(b);

                // zeng: 从datanode中找到的block才有len

                DatanodeInfo node = (DatanodeInfo) containingNodes.first();

                // zeng: 遍历查找,效率很低
                for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
                    Block cur = (Block) it.next();
                    if (b.getBlockId() == cur.getBlockId()) {   // zeng: 找到block
                        b.setNumBytes(cur.getNumBytes());   // zeng: 设置长度
                        break;
                    }
                }

            }

            //
            // Now we can add the (name,blocks) tuple to the filesystem
            //
            if (dir.addFile(src, pendingBlocks)) {  // zeng: 文件加入文件树
                // zeng: 从pendingCreate中移除这个文件对应的vector
                // The file is no longer pending
                pendingCreates.remove(src);

                // zeng: 从pendingCreateBlocks中移除
                for (int i = 0; i < pendingBlocks.length; i++) {
                    pendingCreateBlocks.remove(pendingBlocks[i]);
                }

                // zeng: TODO 租约相关
                synchronized (leases) {
                    Lease lease = (Lease) leases.get(holder);
                    if (lease != null) {
                        lease.completedCreate(src);
                        if (!lease.hasLocks()) {
                            leases.remove(holder);
                            sortedLeases.remove(lease);
                        }
                    }
                }

                //
                // REMIND - mjc - this should be done only after we wait a few secs.
                // The namenode isn't giving datanodes enough time to report the
                // replicated blocks that are automatically done as part of a client
                // write.
                //

                // zeng: 如果block的副本不够, 那么将其放入neededReplications中等待复制
                // Now that the file is real, we need to be sure to replicate
                // the blocks.
                for (int i = 0; i < pendingBlocks.length; i++) {
                    TreeSet containingNodes = (TreeSet) blocksMap.get(pendingBlocks[i]);
                    if (containingNodes.size() < this.desiredReplication) {
                        synchronized (neededReplications) {
                            LOG.info("Completed file " + src + ", at holder " + holder + ".  There is/are only " + containingNodes.size() + " copies of block " + pendingBlocks[i] + ", so replicating up to " + this.desiredReplication);
                            neededReplications.add(pendingBlocks[i]);
                        }
                    }
                }

                return COMPLETE_SUCCESS;
            } else {
                System.out.println("AddFile() for " + src + " failed");
            }
            LOG.info("Dropped through on file add....");
        }

        return OPERATION_FAILED;
    }

    // zeng: 分配该file下的一个block对象

    /**
     * Allocate a block at the given pending filename
     */
    synchronized Block allocateBlock(UTF8 src) {
        Block b = new Block();

        // zeng: 放入pendingCreates中该file对应的vector中
        Vector v = (Vector) pendingCreates.get(src);
        v.add(b);

        // zeng: 放入pendingCreateBlocks中
        pendingCreateBlocks.add(b);

        return b;
    }

    // zeng: 文件已经已经有其他block 并且 其他的block都有足够的副本

    /**
     * Check that the indicated file's blocks are present and
     * replicated.  If not, return false.
     */
    synchronized boolean checkFileProgress(UTF8 src) {
        Vector v = (Vector) pendingCreates.get(src);

        for (Iterator it = v.iterator(); it.hasNext(); ) {
            Block b = (Block) it.next();
            TreeSet containingNodes = (TreeSet) blocksMap.get(b);
            if (containingNodes == null || containingNodes.size() < this.minReplication) {
                return false;
            }
        }

        return true;
    }

    ////////////////////////////////////////////////////////////////
    // Here's how to handle block-copy failure during client write:
    // -- As usual, the client's write should result in a streaming
    // backup write to a k-machine sequence.
    // -- If one of the backup machines fails, no worries.  Fail silently.
    // -- Before client is allowed to close and finalize file, make sure
    // that the blocks are backed up.  Namenode may have to issue specific backup
    // commands to make up for earlier datanode failures.  Once all copies
    // are made, edit namespace and return to client.
    ////////////////////////////////////////////////////////////////

    // zeng: 重命名(实际为移除旧的inode, 加入新的inode)

    /**
     * Change the indicated filename.
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        return dir.renameTo(src, dst);
    }

    // zeng: 文件从文件树中移除, block加入recentInvalidateSets中等待datanode移除

    /**
     * Remove the indicated filename from the namespace.  This may
     * invalidate some blocks that make up the file.
     */
    public synchronized boolean delete(UTF8 src) {
        // zeng: 从文件树中移除, 返回移除的所有block
        Block deletedBlocks[] = (Block[]) dir.delete(src);

        if (deletedBlocks != null) {
            for (int i = 0; i < deletedBlocks.length; i++) {    // zeng: 遍历所有移除的block
                Block b = deletedBlocks[i];

                TreeSet containingNodes = (TreeSet) blocksMap.get(b);   // zeng: block所在的datanode
                if (containingNodes != null) {
                    for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {    // zeng: 遍历datanode

                        DatanodeInfo node = (DatanodeInfo) it.next();

                        // zeng: 获取该datanode对应的一个vector,该vector 存储 最近被移除的block对象
                        Vector invalidateSet = (Vector) recentInvalidateSets.get(node.getName());

                        if (invalidateSet == null) {
                            invalidateSet = new Vector();
                            recentInvalidateSets.put(node.getName(), invalidateSet);
                        }

                        // zeng: block加入该vector
                        invalidateSet.add(b);

                    }
                }

            }
        }

        return (deletedBlocks != null);
    }

    // zeng: 文件是否在文件树中存在

    /**
     * Return whether the given filename exists
     */
    public boolean exists(UTF8 src) {
        if (dir.getFile(src) != null || dir.isDir(src)) {
            return true;
        } else {
            return false;
        }
    }

    // zeng: 文件全描述符是否目录

    /**
     * Whether the given name is a directory
     */
    public boolean isDir(UTF8 src) {
        return dir.isDir(src);
    }

    /**
     * Create all the necessary directories
     */
    public boolean mkdirs(UTF8 src) {
        return dir.mkdirs(src);
    }

    /**
     * Figure out a few hosts that are likely to contain the
     * block(s) referred to by the given (filename, start, len) tuple.
     */
    public UTF8[][] getDatanodeHints(UTF8 src, long start, long len) {
        if (start < 0 || len < 0) {
            return new UTF8[0][];
        }

        int startBlock = -1;
        int endBlock = -1;
        Block blocks[] = dir.getFile(src);

        if (blocks == null) {                     // no blocks
            return new UTF8[0][];
        }

        //
        // First, figure out where the range falls in
        // the blocklist.
        //
        long startpos = start;
        long endpos = start + len;
        for (int i = 0; i < blocks.length; i++) {
            if (startpos >= 0) {
                startpos -= blocks[i].getNumBytes();
                if (startpos <= 0) {
                    startBlock = i;
                }
            }
            if (endpos >= 0) {
                endpos -= blocks[i].getNumBytes();
                if (endpos <= 0) {
                    endBlock = i;
                    break;
                }
            }
        }

        //
        // Next, create an array of hosts where each block can
        // be found
        //
        if (startBlock < 0 || endBlock < 0) {
            return new UTF8[0][];
        } else {
            UTF8 hosts[][] = new UTF8[(endBlock - startBlock) + 1][];
            for (int i = startBlock; i <= endBlock; i++) {
                TreeSet containingNodes = (TreeSet) blocksMap.get(blocks[i]);
                Vector v = new Vector();
                for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    v.add(cur.getHost());
                }
                hosts[i - startBlock] = (UTF8[]) v.toArray(new UTF8[v.size()]);
            }
            return hosts;
        }
    }

    /************************************************************
     * A Lease governs all the locks held by a single client.
     * For each client there's a corresponding lease, whose
     * timestamp is updated when the client periodically
     * checks in.  If the client dies and allows its lease to
     * expire, all the corresponding locks can be released.
     *************************************************************/
    class Lease implements Comparable {
        public UTF8 holder;
        public long lastUpdate;
        TreeSet locks = new TreeSet();
        TreeSet creates = new TreeSet();

        public Lease(UTF8 holder) {
            this.holder = holder;
            renew();
        }

        public void renew() {
            this.lastUpdate = System.currentTimeMillis();
        }

        public boolean expired() {
            if (System.currentTimeMillis() - lastUpdate > LEASE_PERIOD) {
                return true;
            } else {
                return false;
            }
        }

        public void obtained(UTF8 src) {
            locks.add(src);
        }

        public void released(UTF8 src) {
            locks.remove(src);
        }

        public void startedCreate(UTF8 src) {
            creates.add(src);
        }

        public void completedCreate(UTF8 src) {
            creates.remove(src);
        }

        public boolean hasLocks() {
            return (locks.size() + creates.size()) > 0;
        }

        public void releaseLocks() {
            for (Iterator it = locks.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseLock(src, holder);
            }
            locks.clear();
            for (Iterator it = creates.iterator(); it.hasNext(); ) {
                UTF8 src = (UTF8) it.next();
                internalReleaseCreate(src);
            }
            creates.clear();
        }

        /**
         *
         */
        public String toString() {
            return "[Lease.  Holder: " + holder.toString() + ", heldlocks: " + locks.size() + ", pendingcreates: " + creates.size() + "]";
        }

        /**
         *
         */
        public int compareTo(Object o) {
            Lease l1 = (Lease) this;
            Lease l2 = (Lease) o;
            long lu1 = l1.lastUpdate;
            long lu2 = l2.lastUpdate;
            if (lu1 < lu2) {
                return -1;
            } else if (lu1 > lu2) {
                return 1;
            } else {
                return l1.holder.compareTo(l2.holder);
            }
        }
    }

    /******************************************************
     * LeaseMonitor checks for leases that have expired,
     * and disposes of them.
     ******************************************************/
    class LeaseMonitor implements Runnable {
        public void run() {
            while (fsRunning) {

                // zeng: TODO
                synchronized (FSNamesystem.this) {
                    synchronized (leases) {
                        Lease top;
                        while ((sortedLeases.size() > 0) &&
                                ((top = (Lease) sortedLeases.first()) != null)) {
                            if (top.expired()) {
                                top.releaseLocks();
                                leases.remove(top.holder);
                                LOG.info("Removing lease " + top + ", leases remaining: " + sortedLeases.size());
                                if (!sortedLeases.remove(top)) {
                                    LOG.info("Unknown failure trying to remove " + top + " from lease set.");
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Get a lock (perhaps exclusive) on the given file
     */
    public synchronized int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        int result = dir.obtainLock(src, holder, exclusive);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease == null) {
                    lease = new Lease(holder);
                    leases.put(holder, lease);
                    sortedLeases.add(lease);
                } else {
                    sortedLeases.remove(lease);
                    lease.renew();
                    sortedLeases.add(lease);
                }
                lease.obtained(src);
            }
        }
        return result;
    }

    /**
     * Release the lock on the given file
     */
    public synchronized int releaseLock(UTF8 src, UTF8 holder) {
        int result = internalReleaseLock(src, holder);
        if (result == COMPLETE_SUCCESS) {
            synchronized (leases) {
                Lease lease = (Lease) leases.get(holder);
                if (lease != null) {
                    lease.released(src);
                    if (!lease.hasLocks()) {
                        leases.remove(holder);
                        sortedLeases.remove(lease);
                    }
                }
            }
        }
        return result;
    }

    private int internalReleaseLock(UTF8 src, UTF8 holder) {
        return dir.releaseLock(src, holder);
    }

    private void internalReleaseCreate(UTF8 src) {
        Vector v = (Vector) pendingCreates.remove(src);
        for (Iterator it2 = v.iterator(); it2.hasNext(); ) {
            Block b = (Block) it2.next();
            pendingCreateBlocks.remove(b);
        }
    }

    /**
     * Renew the lease(s) held by the given client
     */
    public void renewLease(UTF8 holder) {
        synchronized (leases) {
            Lease lease = (Lease) leases.get(holder);
            if (lease != null) {
                sortedLeases.remove(lease);
                lease.renew();
                sortedLeases.add(lease);
            }
        }
    }

    // zeng: 获取目录下所有本级节点信息

    /**
     * Get a listing of all files at 'src'.  The Object[] array
     * exists so we can return file attributes (soon to be implemented)
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        return dir.getListing(src);
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by datanodes
    //
    /////////////////////////////////////////////////////////

    // zeng: 接收到datanode心跳, 更新datanode相关信息

    /**
     * The given node has reported in.  This method should:
     * 1) Record the heartbeat, so the datanode isn't timed out
     * 2) Adjust usage stats for future block allocation
     */
    public synchronized void gotHeartbeat(UTF8 name, long capacity, long remaining) {
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                long capacityDiff = 0;
                long remainingDiff = 0;

                DatanodeInfo nodeinfo = (DatanodeInfo) datanodeMap.get(name);

                if (nodeinfo == null) {
                    LOG.info("Got brand-new heartbeat from " + name);

                    // zeng: DatanodeInfo对象
                    nodeinfo = new DatanodeInfo(name, capacity, remaining);
                    datanodeMap.put(name, nodeinfo);

                    // zeng: 容量变更
                    capacityDiff = capacity;
                    remainingDiff = remaining;
                } else {
                    // zeng: 容量变更
                    capacityDiff = capacity - nodeinfo.getCapacity();
                    remainingDiff = remaining - nodeinfo.getRemaining();

                    heartbeats.remove(nodeinfo);

                    // zeng: 更新DatanodeInfo
                    nodeinfo.updateHeartbeat(capacity, remaining);
                }

                // zeng: 心跳的机器对应的DatanodeInfo加入hearbeats
                heartbeats.add(nodeinfo);

                // zeng: 更新容量
                totalCapacity += capacityDiff;
                totalRemaining += remainingDiff;
            }
        }
    }

    /**
     * Periodically calls heartbeatCheck().
     */
    class HeartbeatMonitor implements Runnable {
        /**
         *
         */
        public void run() {
            while (fsRunning) {
                // zeng: 心跳检测
                heartbeatCheck();

                try {
                    Thread.sleep(heartBeatRecheck);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Check if there are any expired heartbeats, and if so,
     * whether any blocks have to be re-replicated.
     */
    synchronized void heartbeatCheck() {
        synchronized (heartbeats) {
            DatanodeInfo nodeInfo = null;

            while ((heartbeats.size() > 0) &&
                    ((nodeInfo = (DatanodeInfo) heartbeats.first()) != null) &&
                    (nodeInfo.lastUpdate() < System.currentTimeMillis() - EXPIRE_INTERVAL)
            ) { // zeng: 10分钟没心跳
                LOG.info("Lost heartbeat for " + nodeInfo.getName());

                // zeng: 从heartbeats中移除
                heartbeats.remove(nodeInfo);

                // zeng: 从 datanode名称 -> DatanodeInfo对象 映射 中移除
                synchronized (datanodeMap) {
                    datanodeMap.remove(nodeInfo.getName());
                }

                // zeng: 更新总容量
                totalCapacity -= nodeInfo.getCapacity();
                // zeng: 更新剩余容量
                totalRemaining -= nodeInfo.getRemaining();

                // zeng: 这个datanode下所有的block
                Block deadblocks[] = nodeInfo.getBlocks();
                if (deadblocks != null) {
                    for (int i = 0; i < deadblocks.length; i++) {
                        // zeng: 从  block -> datanode set 映射的 set中 移除
                        removeStoredBlock(deadblocks[i], nodeInfo);
                    }
                }

                if (heartbeats.size() > 0) {
                    nodeInfo = (DatanodeInfo) heartbeats.first();
                }
            }
        }
    }

    // zeng: datanode上报所有block信息

    /**
     * The given node is reporting all its blocks.  Use this info to
     * update the (machine-->blocklist) and (block-->machinelist) tables.
     */
    public synchronized Block[] processReport(Block newReport[], UTF8 name) {
        // zeng: 根据datanode名称获取DatanodeInfo对象
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);

        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Received block report from node " + name + ", but there is no info for " + name);
        }

        // zeng: 对比 上报的block数组 和 DatanodeInfo.blocks 数组, 更新 block -> datanode set 映射
        //
        // Modify the (block-->datanode) map, according to the difference
        // between the old and new block report.
        //
        int oldPos = 0, newPos = 0;
        Block oldReport[] = node.getBlocks();
        while (oldReport != null && newReport != null && oldPos < oldReport.length && newPos < newReport.length) {
            int cmp = oldReport[oldPos].compareTo(newReport[newPos]);

            if (cmp == 0) {
                // zeng: block没变 下一个block
                // Do nothing, blocks are the same
                oldPos++;
                newPos++;
            } else if (cmp < 0) {
                // zeng: 有删除的block
                // The old report has a block the new one does not
                removeStoredBlock(oldReport[oldPos], node);
                oldPos++;
            } else {
                // zeng: 有新增的block
                // The new report has a block the old one does not
                addStoredBlock(newReport[newPos], node);
                newPos++;
            }
        }
        // zeng: 剩下的全是删除的block
        while (oldReport != null && oldPos < oldReport.length) {
            // The old report has a block the new one does not
            removeStoredBlock(oldReport[oldPos], node);
            oldPos++;
        }
        // zeng: 剩下的全是新增的block
        while (newReport != null && newPos < newReport.length) {
            // The new report has a block the old one does not
            addStoredBlock(newReport[newPos], node);
            newPos++;
        }

        // zeng: 更新DatanodeInfo.blocks
        //
        // Modify node so it has the new blockreport
        //
        node.updateBlocks(newReport);

        // zeng: 无效的block返回给datanode进行删除

        //
        // We've now completely updated the node's block report profile.
        // We now go through all its blocks and find which ones are invalid,
        // no longer pending, or over-replicated.
        //
        // (Note it's not enough to just invalidate blocks at lease expiry 
        // time; datanodes can go down before the client's lease on 
        // the failed file expires and miss the "expire" event.)
        //
        // This function considers every block on a datanode, and thus
        // should only be invoked infrequently.
        //
        Vector obsolete = new Vector();
        for (Iterator it = node.getBlockIterator(); it.hasNext(); ) {
            Block b = (Block) it.next();

            if (!dir.isValidBlock(b) && !pendingCreateBlocks.contains(b)) { // zeng: 无效的block
                LOG.info("Obsoleting block " + b);
                obsolete.add(b);
            }
        }

        return (Block[]) obsolete.toArray(new Block[obsolete.size()]);
    }

    // zeng: 加入  block -> datanode set 映射中

    /**
     * Modify (block-->datanode) map.  Remove block from set of
     * needed replications if this takes care of the problem.
     */
    synchronized void addStoredBlock(Block block, DatanodeInfo node) {

        // zeng:  block -> datanode set 映射, 加入 datanode set中

        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null) {
            containingNodes = new TreeSet();
            blocksMap.put(block, containingNodes);
        }
        if (!containingNodes.contains(node)) {
            containingNodes.add(node);
        } else {
            LOG.info("Redundant addStoredBlock request received for block " + block + " on node " + node);
        }

        synchronized (neededReplications) {
            if (dir.isValidBlock(block)) {  // zeng: 是否是文件树中的block

                // zeng: 副本足够了就从neededReplications中移除, 副本不足加加入neededReplications中

                if (containingNodes.size() >= this.desiredReplication) {
                    neededReplications.remove(block);
                    pendingReplications.remove(block);
                } else if (containingNodes.size() < this.desiredReplication) {
                    if (!neededReplications.contains(block)) {
                        neededReplications.add(block);
                    }
                }

                // zeng: 如果block当前副本数超出, 移除多余副本

                //
                // Find how many of the containing nodes are "extra", if any.
                // If there are any extras, call chooseExcessReplicates() to
                // mark them in the excessReplicateMap.
                //
                Vector nonExcess = new Vector();
                for (Iterator it = containingNodes.iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getName());
                    if (excessBlocks == null || !excessBlocks.contains(block)) {
                        nonExcess.add(cur);
                    }
                }
                if (nonExcess.size() > this.maxReplication) {
                    chooseExcessReplicates(nonExcess, block, this.maxReplication);
                }
            }
        }
    }

    // zeng: 加入 `多余block副本`set 与 `要删除的block副本`set

    /**
     * We want a max of "maxReps" replicates for any block, but we now have too many.
     * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
     * <p>
     * srcNodes.size() - dstNodes.size() == maxReps
     * <p>
     * For now, we choose nodes randomly.  In the future, we might enforce some
     * kind of policy (like making sure replicates are spread across racks).
     */
    void chooseExcessReplicates(Vector nonExcess, Block b, int maxReps) {
        while (nonExcess.size() - maxReps > 0) {    // zeng: 只保留maxReps个副本

            // zeng: DatanodeInfo
            int chosenNode = r.nextInt(nonExcess.size());
            DatanodeInfo cur = (DatanodeInfo) nonExcess.elementAt(chosenNode);
            nonExcess.removeElementAt(chosenNode);

            // zeng: 加入 多余block副本set 中
            TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(cur.getName());
            if (excessBlocks == null) {
                excessBlocks = new TreeSet();
                excessReplicateMap.put(cur.getName(), excessBlocks);
            }
            excessBlocks.add(b);

            // zeng: 加入 要删除的block副本set 中
            //
            // The 'excessblocks' tracks blocks until we get confirmation
            // that the datanode has deleted them; the only way we remove them
            // is when we get a "removeBlock" message.  
            //
            // The 'invalidate' list is used to inform the datanode the block 
            // should be deleted.  Items are removed from the invalidate list
            // upon giving instructions to the namenode.
            //
            Vector invalidateSet = (Vector) recentInvalidateSets.get(cur.getName());
            if (invalidateSet == null) {
                invalidateSet = new Vector();
                recentInvalidateSets.put(cur.getName(), invalidateSet);
            }
            invalidateSet.add(b);
        }
    }

    // zeng: 从  block -> datanode set 映射的 set中 移除

    /**
     * Modify (block-->datanode) map.  Possibly generate
     * replication tasks, if the removed block is still valid.
     */
    synchronized void removeStoredBlock(Block block, DatanodeInfo node) {
        // zeng: 从 block -> datanode set 映射 的set中移除
        TreeSet containingNodes = (TreeSet) blocksMap.get(block);
        if (containingNodes == null || !containingNodes.contains(node)) {
            throw new IllegalArgumentException("No machine mapping found for block " + block + ", which should be at node " + node);
        }
        containingNodes.remove(node);

        // zeng: block移除可能是因为datanode出问题导致的, 这种情况下block要加进neededReplications等待复制
        //
        // It's possible that the block was removed because of a datanode
        // failure.  If the block is still valid, check if replication is
        // necessary.  In that case, put block on a possibly-will-
        // be-replicated list.
        //
        if (dir.isValidBlock(block) && (containingNodes.size() < this.desiredReplication)) {
            synchronized (neededReplications) {
                neededReplications.add(block);
            }
        }

        // zeng: block已经移除, 那么ta肯定不会在excessReplicateMap(datanode名称 -> 多余block副本 set)里
        //
        // We've removed a block from a node, so it's definitely no longer
        // in "excess" there.
        //
        TreeSet excessBlocks = (TreeSet) excessReplicateMap.get(node.getName());
        if (excessBlocks != null) {
            excessBlocks.remove(block);
            if (excessBlocks.size() == 0) {
                excessReplicateMap.remove(node.getName());
            }
        }
    }

    // zeng: datanode已经完成block存储, namenode做相关处理

    /**
     * The given node is reporting that it received a certain block.
     */
    public synchronized void blockReceived(Block block, UTF8 name) {
        // zeng: 根据名称获取DatanodeInfo对象
        DatanodeInfo node = (DatanodeInfo) datanodeMap.get(name);

        if (node == null) {
            throw new IllegalArgumentException("Unexpected exception.  Got blockReceived message from node " + name + ", but there is no info for " + name);
        }

        // zeng: 加入  block -> datanode set 映射中
        //
        // Modify the blocks->datanode map
        // 
        addStoredBlock(block, node);


        // zeng: 放入DatanodeInfo.blocks这个set中
        //
        // Supplement node's blockreport
        //
        node.addBlock(block);
    }

    /**
     * Total raw bytes
     */
    public long totalCapacity() {
        return totalCapacity;
    }

    /**
     * Total non-used raw bytes
     */
    public long totalRemaining() {
        return totalRemaining;
    }

    /**
     *
     */
    public DatanodeInfo[] datanodeReport() {
        DatanodeInfo results[] = null;
        synchronized (heartbeats) {
            synchronized (datanodeMap) {
                results = new DatanodeInfo[datanodeMap.size()];
                int i = 0;
                for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {
                    DatanodeInfo cur = (DatanodeInfo) it.next();
                    results[i++] = cur;
                }
            }
        }
        return results;
    }

    /////////////////////////////////////////////////////////
    //
    // These methods are called by the Namenode system, to see
    // if there is any work for a given datanode.
    //
    /////////////////////////////////////////////////////////

    // zeng: 取出 要删除的block副本set

    /**
     * Check if there are any recently-deleted blocks a datanode should remove.
     */
    public synchronized Block[] blocksToInvalidate(UTF8 sender) {
        // zeng: 取出 要删除的block副本set
        Vector invalidateSet = (Vector) recentInvalidateSets.remove(sender);

        if (invalidateSet != null) {

            // zeng: 返回
            return (Block[]) invalidateSet.toArray(new Block[invalidateSet.size()]);
        } else {
            return null;
        }
    }

    // zeng: 需要复制的block, 及block需要复制到哪些datanode

    /**
     * Return with a list of Block/DataNodeInfo sets, indicating
     * where various Blocks should be copied, ASAP.
     * <p>
     * The Array that we return consists of two objects:
     * The 1st elt is an array of Blocks.
     * The 2nd elt is a 2D array of DatanodeInfo objs, identifying the
     * target sequence for the Block at the appropriate index.
     */
    public synchronized Object[] pendingTransfers(DatanodeInfo srcNode, int xmitsInProgress) {
        synchronized (neededReplications) {
            Object results[] = null;
            int scheduledXfers = 0;

            if (neededReplications.size() > 0) {
                //
                // Go through all blocks that need replications.  See if any
                // are present at the current node.  If so, ask the node to
                // replicate them.
                //
                Vector replicateBlocks = new Vector();
                Vector replicateTargetSets = new Vector();

                for (Iterator it = neededReplications.iterator(); it.hasNext(); ) { // zeng: 遍历需要复制的block

                    // zeng: 不能超过datanode的复制任务上限
                    //
                    // We can only reply with 'maxXfers' or fewer blocks
                    //
                    if (scheduledXfers >= this.maxReplicationStreams - xmitsInProgress) {
                        break;
                    }

                    // zeng: 要复制的block
                    Block block = (Block) it.next();
                    if (!dir.isValidBlock(block)) {
                        it.remove();
                    } else {
                        TreeSet containingNodes = (TreeSet) blocksMap.get(block);

                        // zeng: datanode里包括这个block
                        if (containingNodes.contains(srcNode)) {
                            // zeng: 选择不包含这个block的datanode
                            DatanodeInfo targets[] = chooseTargets(Math.min(this.desiredReplication - containingNodes.size(), this.maxReplicationStreams - xmitsInProgress), containingNodes, null);

                            if (targets.length > 0) {
                                // Build items to return
                                // zeng: 需要复制的block
                                replicateBlocks.add(block);
                                // zeng: 复制到哪些datanode
                                replicateTargetSets.add(targets);

                                // zeng: 几个复制任务
                                scheduledXfers += targets.length;
                            }

                        }
                    }
                }

                //
                // Move the block-replication into a "pending" state.
                // The reason we use 'pending' is so we can retry
                // replications that fail after an appropriate amount of time.  
                // (REMIND - mjc - this timer is not yet implemented.)
                //
                if (replicateBlocks.size() > 0) {
                    int i = 0;

                    // zeng: block从neededReplications中移除, 加入pendingReplications中
                    for (Iterator it = replicateBlocks.iterator(); it.hasNext(); i++) {
                        Block block = (Block) it.next();
                        DatanodeInfo targets[] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);

                        TreeSet containingNodes = (TreeSet) blocksMap.get(block);

                        if (containingNodes.size() + targets.length >= this.desiredReplication) {   // zeng: 大于指定副本数
                            neededReplications.remove(block);
                            pendingReplications.add(block);
                        }

                        LOG.info("Pending transfer (block " + block.getBlockName() + ") from " + srcNode.getName() + " to " + targets.length + " destinations");
                    }


                    //
                    // Build returned objects from above lists
                    //

                    // zeng: `block的target列表`数组 转化为 二维数组
                    DatanodeInfo targetMatrix[][] = new DatanodeInfo[replicateTargetSets.size()][];
                    for (i = 0; i < targetMatrix.length; i++) {
                        targetMatrix[i] = (DatanodeInfo[]) replicateTargetSets.elementAt(i);
                    }

                    results = new Object[2];

                    // zeng: 需要复制的block
                    results[0] = replicateBlocks.toArray(new Block[replicateBlocks.size()]);
                    // zeng: block复制到哪些datanode
                    results[1] = targetMatrix;
                }

            }

            return results;
        }
    }

    // zeng: 为block分配datanode

    /**
     * Get a certain number of targets, if possible.
     * If not, return as many as we can.
     *
     * @param desiredReplicates number of duplicates wanted.
     * @param forbiddenNodes    of DatanodeInfo instances that should not be
     *                          considered targets.
     * @return array of DatanodeInfo instances uses as targets.
     */
    DatanodeInfo[] chooseTargets(int desiredReplicates, TreeSet forbiddenNodes, UTF8 clientMachine) {
        TreeSet alreadyChosen = new TreeSet();
        Vector targets = new Vector();

        for (int i = 0; i < desiredReplicates; i++) {
            // zeng: 分配一个datanode
            DatanodeInfo target = chooseTarget(forbiddenNodes, alreadyChosen, clientMachine);

            if (target != null) {
                targets.add(target);
                alreadyChosen.add(target);
            } else {
                break; // calling chooseTarget again won't help
            }
        }
        return (DatanodeInfo[]) targets.toArray(new DatanodeInfo[targets.size()]);
    }

    // zeng: 分配一个datanode

    /**
     * Choose a target from available machines, excepting the
     * given ones.
     * <p>
     * Right now it chooses randomly from available boxes.  In future could
     * choose according to capacity and load-balancing needs (or even
     * network-topology, to avoid inter-switch traffic).
     *
     * @param forbidden1 DatanodeInfo targets not allowed, null allowed.
     * @param forbidden2 DatanodeInfo targets not allowed, null allowed.
     * @return DatanodeInfo instance to use or null if something went wrong
     * (a log message is emitted if null is returned).
     */
    DatanodeInfo chooseTarget(TreeSet forbidden1, TreeSet forbidden2, UTF8 clientMachine) {
        //
        // Check if there are any available targets at all
        //
        int totalMachines = datanodeMap.size();
        if (totalMachines == 0) {
            LOG.warning("While choosing target, totalMachines is " + totalMachines);
            return null;
        }

        // zeng: 哪些机器不能选
        //
        // Build a map of forbidden hostnames from the two forbidden sets.
        //
        TreeSet forbiddenMachines = new TreeSet();
        if (forbidden1 != null) {
            for (Iterator it = forbidden1.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }
        if (forbidden2 != null) {
            for (Iterator it = forbidden2.iterator(); it.hasNext(); ) {
                DatanodeInfo cur = (DatanodeInfo) it.next();
                forbiddenMachines.add(cur.getHost());
            }
        }

        // zeng: 遍历datanodeMap, 选出所有满足条件的

        //
        // Build list of machines we can actually choose from
        //
        Vector targetList = new Vector();
        for (Iterator it = datanodeMap.values().iterator(); it.hasNext(); ) {

            DatanodeInfo node = (DatanodeInfo) it.next();
            if (!forbiddenMachines.contains(node.getHost())) {
                targetList.add(node);
            }

        }
        // zeng: shuffle
        Collections.shuffle(targetList);

        //
        // Now pick one
        //
        if (targetList.size() > 0) {

            // zeng: 先看看请求客户端所在的机器是否满足条件
            //
            // If the requester's machine is in the targetList, 
            // and it's got the capacity, pick it.
            //
            if (clientMachine != null && clientMachine.getLength() > 0) {
                for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                    DatanodeInfo node = (DatanodeInfo) it.next();
                    if (clientMachine.equals(node.getHost())) {
                        if (node.getRemaining() > BLOCK_SIZE * MIN_BLOCKS_FOR_WRITE) {  // zeng: 剩余空间 大于 5 * 32M
                            return node;
                        }
                    }
                }
            }

            // zeng: 再看看是否存在  `剩余空间 大于 5 * 32M` 的机器
            //
            // Otherwise, choose node according to target capacity
            //
            for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
                if (node.getRemaining() > BLOCK_SIZE * MIN_BLOCKS_FOR_WRITE) {
                    return node;
                }
            }

            // zeng: 如果都没有,就看看是否存在  `剩余空间 大于 32M` 的机器
            //
            // That should do the trick.  But we might not be able
            // to pick any node if the target was out of bytes.  As
            // a last resort, pick the first valid one we can find.
            //
            for (Iterator it = targetList.iterator(); it.hasNext(); ) {
                DatanodeInfo node = (DatanodeInfo) it.next();
                if (node.getRemaining() > BLOCK_SIZE) {
                    return node;
                }
            }

            LOG.warning("Could not find any nodes with sufficient capacity");
            return null;

        } else {
            LOG.warning("Zero targets found, forbidden1.size=" +
                    (forbidden1 != null ? forbidden1.size() : 0) +
                    " forbidden2.size()=" +
                    (forbidden2 != null ? forbidden2.size() : 0));
            return null;
        }
    }
}
