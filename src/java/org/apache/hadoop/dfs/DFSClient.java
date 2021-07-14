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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and 
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects 
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of 
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 * @author Mike Cafarella, Tessa MacDuff
 ********************************************************/
class DFSClient implements FSConstants {
    public static final Logger LOG = LogFormatter.getLogger("org.apache.hadoop.fs.DFSClient");
    static int MAX_BLOCK_ACQUIRE_FAILURES = 3;
    ClientProtocol namenode;
    String localName;
    boolean running = true;
    Random r = new Random();
    String clientName;
    Daemon leaseChecker;
    private Configuration conf;

    /**
     * Create a new DFSClient connected to the given namenode server.
     */
    public DFSClient(InetSocketAddress nameNodeAddr, Configuration conf) {
        this.conf = conf;

        // zeng: nameode rpc client
        this.namenode = (ClientProtocol) RPC.getProxy(ClientProtocol.class, nameNodeAddr, conf);

        try {
            // zeng: 本机域名
            this.localName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException uhe) {
            this.localName = "";
        }

        // zeng: DFSClient_随机数
        this.clientName = "DFSClient_" + r.nextInt();

        // zeng: TODO
        this.leaseChecker = new Daemon(new LeaseChecker());
        this.leaseChecker.start();
    }

    /**
     *
     */
    public void close() throws IOException {
        this.running = false;
        try {
            leaseChecker.join();
        } catch (InterruptedException ie) {
        }
    }

    /**
     * Get hints about the location of the indicated block(s).  The
     * array returned is as long as there are blocks in the indicated
     * range.  Each block may have one or more locations.
     */
    public String[][] getHints(UTF8 src, long start, long len) throws IOException {
        return namenode.getHints(src.toString(), start, len);
    }

    /**
     * Create an input stream that obtains a nodelist from the
     * namenode, and then reads from all the right places.  Creates
     * inner subclass of InputStream that does the right out-of-band
     * work.
     */
    public FSInputStream open(UTF8 src) throws IOException {
        // Get block info from namenode
        return new DFSInputStream(src.toString());
    }

    public FSOutputStream create(UTF8 src, boolean overwrite) throws IOException {
        // zeng: dfs outputstream
        return new DFSOutputStream(src, overwrite);
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean rename(UTF8 src, UTF8 dst) throws IOException {
        return namenode.rename(src.toString(), dst.toString());
    }

    /**
     * Make a direct connection to namenode and manipulate structures
     * there.
     */
    public boolean delete(UTF8 src) throws IOException {
        return namenode.delete(src.toString());
    }

    /**
     *
     */
    public boolean exists(UTF8 src) throws IOException {
        // zeng: 请求namenode,判断文件是否存在
        return namenode.exists(src.toString());
    }

    /**
     *
     */
    public boolean isDirectory(UTF8 src) throws IOException {
        return namenode.isDir(src.toString());
    }

    /**
     *
     */
    public DFSFileInfo[] listFiles(UTF8 src) throws IOException {
        return namenode.getListing(src.toString());
    }

    /**
     *
     */
    public long totalRawCapacity() throws IOException {
        long rawNums[] = namenode.getStats();
        return rawNums[0];
    }

    /**
     *
     */
    public long totalRawUsed() throws IOException {
        long rawNums[] = namenode.getStats();
        return rawNums[1];
    }

    public DatanodeInfo[] datanodeReport() throws IOException {
        return namenode.getDatanodeReport();
    }

    /**
     *
     */
    public boolean mkdirs(UTF8 src) throws IOException {
        return namenode.mkdirs(src.toString());
    }

    /**
     *
     */
    public void lock(UTF8 src, boolean exclusive) throws IOException {
        long start = System.currentTimeMillis();
        boolean hasLock = false;
        while (!hasLock) {
            hasLock = namenode.obtainLock(src.toString(), clientName, exclusive);
            if (!hasLock) {
                try {
                    Thread.sleep(400);
                    if (System.currentTimeMillis() - start > 5000) {
                        LOG.info("Waiting to retry lock for " + (System.currentTimeMillis() - start) + " ms.");
                        Thread.sleep(2000);
                    }
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     *
     */
    public void release(UTF8 src) throws IOException {
        boolean hasReleased = false;
        while (!hasReleased) {
            hasReleased = namenode.releaseLock(src.toString(), clientName);
            if (!hasReleased) {
                LOG.info("Could not release.  Retrying...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    // zeng: 从datanode数组中选一个
    /**
     * Pick the best node from which to stream the data.
     * That's the local one, if available.
     */
    private DatanodeInfo bestNode(DatanodeInfo nodes[], TreeSet deadNodes) throws IOException {
        if ((nodes == null) ||
                (nodes.length - deadNodes.size() < 1)) {
            throw new IOException("No live nodes contain current block");
        }
        DatanodeInfo chosenNode = null;

        // zeng: 优先本地机器
        for (int i = 0; i < nodes.length; i++) {
            // zeng: 不在deadNodes中
            if (deadNodes.contains(nodes[i])) {
                continue;
            }

            // zeng: host
            String nodename = nodes[i].getName().toString();
            int colon = nodename.indexOf(':');
            if (colon >= 0) {
                nodename = nodename.substring(0, colon);
            }

            // zeng: 如果是本地机器
            if (localName.equals(nodename)) {
                chosenNode = nodes[i];
                break;
            }
        }

        // zeng: 从其他节点随便选一个
        if (chosenNode == null) {
            do {
                chosenNode = nodes[Math.abs(r.nextInt()) % nodes.length];
            } while (deadNodes.contains(chosenNode));
        }

        return chosenNode;
    }

    // zeng: TODO 租约?

    /***************************************************************
     * Periodically check in with the namenode and renew all the leases
     * when the lease period is half over.
     ***************************************************************/
    class LeaseChecker implements Runnable {
        /**
         *
         */
        public void run() {
            long lastRenewed = 0;
            while (running) {
                if (System.currentTimeMillis() - lastRenewed > (LEASE_PERIOD / 2)) {
                    try {
                        namenode.renewLease(clientName);
                        lastRenewed = System.currentTimeMillis();
                    } catch (IOException ie) {
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /****************************************************************
     * DFSInputStream provides bytes from a named file.  It handles 
     * negotiation of the namenode and various datanodes as necessary.
     ****************************************************************/
    class DFSInputStream extends FSInputStream {
        private Socket s = null;
        boolean closed = false;

        private String src;
        private DataInputStream blockStream;
        private Block blocks[] = null;
        private DatanodeInfo nodes[][] = null;
        private long pos = 0;
        private long filelen = 0;
        private long blockEnd = -1;

        /**
         *
         */
        public DFSInputStream(String src) throws IOException {
            this.src = src;

            // zeng: 从namenode获取文件信息
            openInfo();

            this.blockStream = null;

            // zeng: 统计file大小
            for (int i = 0; i < blocks.length; i++) {
                this.filelen += blocks[i].getNumBytes();
            }
        }

        // zeng: 从namenode获取文件信息
        /**
         * Grab the open-file info from namenode
         */
        void openInfo() throws IOException {
            Block oldBlocks[] = this.blocks;

            // zeng: 返回文件包含哪些block, block保存在哪些datanode
            LocatedBlock results[] = namenode.open(src);

            Vector blockV = new Vector();
            Vector nodeV = new Vector();
            for (int i = 0; i < results.length; i++) {
                blockV.add(results[i].getBlock());
                nodeV.add(results[i].getLocations());
            }

            Block newBlocks[] = (Block[]) blockV.toArray(new Block[blockV.size()]);

            if (oldBlocks != null) {
                for (int i = 0; i < oldBlocks.length; i++) {
                    if (!oldBlocks[i].equals(newBlocks[i])) {
                        throw new IOException("Blocklist for " + src + " has changed!");
                    }
                }
                if (oldBlocks.length != newBlocks.length) {
                    throw new IOException("Blocklist for " + src + " now has different length");
                }
            }

            // zeng: 文件下有哪些block
            this.blocks = newBlocks;
            // zeng: 每个block在保存哪些datanode下
            this.nodes = (DatanodeInfo[][]) nodeV.toArray(new DatanodeInfo[nodeV.size()][]);
        }

        // zeng: 文件中的target位置对应哪个block, 获取block input stream
        /**
         * Open a DataInputStream to a DataNode so that it can be read from.
         * We get block ID and the IDs of the destinations at startup, from the namenode.
         */
        private synchronized void blockSeekTo(long target) throws IOException {
            // zeng: 超出文件大小了
            if (target >= filelen) {
                throw new IOException("Attempted to read past end of file");
            }

            if (s != null) {
                s.close();
                s = null;
            }

            //
            // Compute desired block
            //
            int targetBlock = -1;
            long targetBlockStart = 0;
            long targetBlockEnd = 0;

            // zeng: 文件中target位置的字节在哪个block上
            for (int i = 0; i < blocks.length; i++) {
                // zeng: block len
                long blocklen = blocks[i].getNumBytes();

                // zeng: block end
                targetBlockEnd = targetBlockStart + blocklen - 1;

                if (target >= targetBlockStart && target <= targetBlockEnd) {   // zeng: hit
                    targetBlock = i;
                    break;
                } else {
                    targetBlockStart = targetBlockEnd + 1;  // zeng: next block start
                }
            }

            if (targetBlock < 0) {
                throw new IOException("Impossible situation: could not find target position " + target);
            }

            // zeng: 从这个block什么位置开始读
            long offsetIntoBlock = target - targetBlockStart;

            //
            // Connect to best DataNode for desired Block, with potential offset
            //
            int failures = 0;
            InetSocketAddress targetAddr = null;
            TreeSet deadNodes = new TreeSet();

            while (s == null) {
                DatanodeInfo chosenNode;

                try {
                    // zeng: 从datanode数组中选一个
                    chosenNode = bestNode(nodes[targetBlock], deadNodes);

                    targetAddr = DataNode.createSocketAddr(chosenNode.getName().toString());

                } catch (IOException ie) {
                    String blockInfo =
                            blocks[targetBlock] + " file=" + src + " offset=" + target;

                    // zeng: 三次重试
                    if (failures >= MAX_BLOCK_ACQUIRE_FAILURES) {
                        throw new IOException("Could not obtain block: " + blockInfo);
                    }

                    if (nodes[targetBlock] == null || nodes[targetBlock].length == 0) {
                        LOG.info("No node available for block: " + blockInfo);
                    }

                    LOG.info("Could not obtain block from any node:  " + ie);

                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException iex) {
                    }
                    deadNodes.clear();

                    // zeng: 重新从namenode获取文件信息
                    openInfo();

                    failures++;
                    continue;
                }

                try {
                    // zeng: datanode socket
                    s = new Socket();
                    s.connect(targetAddr, READ_TIMEOUT);
                    s.setSoTimeout(READ_TIMEOUT);

                    //
                    // Xmit header info to datanode
                    //
                    // zeng: datanode outputstream
                    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));

                    // zeng: 发送操作码
                    out.write(OP_READSKIP_BLOCK);

                    // zeng: 发送block对象
                    blocks[targetBlock].write(out);
                    // zeng: 跳过多少个字节
                    out.writeLong(offsetIntoBlock);
                    // zeng: flush
                    out.flush();

                    //
                    // Get bytes in block, set streams
                    //
                    // zeng: socket inputstream
                    DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));

                    // zeng: block size
                    long curBlockSize = in.readLong();
                    // zeng: 实际跳过多少字节
                    long amtSkipped = in.readLong();

                    if (curBlockSize != blocks[targetBlock].len) {
                        throw new IOException("Recorded block size is " + blocks[targetBlock].len + ", but datanode reports size of " + curBlockSize);
                    }
                    if (amtSkipped != offsetIntoBlock) {
                        throw new IOException("Asked for offset of " + offsetIntoBlock + ", but only received offset of " + amtSkipped);
                    }

                    // zeng: 文件中target位置
                    this.pos = target;
                    // zeng: 这个block的结束位置相当于文件中什么位置
                    this.blockEnd = targetBlockEnd;
                    // zeng: block input stream
                    this.blockStream = in;
                } catch (IOException ex) {
                    // Put chosen node into dead list, continue
                    LOG.info("Failed to connect to " + targetAddr + ":" + ex);
                    deadNodes.add(chosenNode);
                    if (s != null) {
                        try {
                            s.close();
                        } catch (IOException iex) {
                        }
                    }
                    s = null;
                }
            }
        }

        /**
         * Close it down!
         */
        public synchronized void close() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            // zeng: close block stream
            if (s != null) {
                blockStream.close();
                s.close();
                s = null;
            }

            super.close();

            closed = true;
        }

        /**
         * Basic read()
         */
        public synchronized int read() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            int result = -1;
            if (pos < filelen) {
                if (pos > blockEnd) {
                    blockSeekTo(pos);
                }

                result = blockStream.read();

                if (result >= 0) {
                    pos++;
                }
            }

            return result;
        }

        /**
         * Read the entire buffer.
         */
        public synchronized int read(byte buf[], int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (pos < filelen) {
                if (pos > blockEnd) {   // zeng: 需要读取下一个block了
                    blockSeekTo(pos);
                }

                // zeng: 读取到buf
                // zeng: `blockEnd - pos + 1`为本block剩余要读字节数
                int result = blockStream.read(buf, off, Math.min(len, (int) (blockEnd - pos + 1)));

                // zeng: file read next pos
                if (result >= 0) {
                    pos += result;
                }

                return result;
            }

            return -1;
        }

        /**
         * Seek to a new arbitrary location
         */
        public synchronized void seek(long targetPos) throws IOException {
            if (targetPos >= filelen) {
                throw new IOException("Cannot seek after EOF");
            }
            pos = targetPos;
            blockEnd = -1;
        }

        /**
         *
         */
        public synchronized long getPos() throws IOException {
            return pos;
        }

        /**
         *
         */
        public synchronized int available() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            return (int) (filelen - pos);
        }

        /**
         * We definitely don't support marks
         */
        public boolean markSupported() {
            return false;
        }

        public void mark(int readLimit) {
        }

        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }
    }

    /****************************************************************
     * DFSOutputStream creates files from a stream of bytes.
     ****************************************************************/
    class DFSOutputStream extends FSOutputStream {
        private Socket s;
        boolean closed = false;

        private byte outBuf[] = new byte[BUFFER_SIZE];
        private int pos = 0;

        private UTF8 src;
        private boolean overwrite;
        private boolean firstTime = true;
        private DataOutputStream blockStream;
        private DataInputStream blockReplyStream;
        private File backupFile;
        private OutputStream backupStream;
        private Block block;
        private long filePos = 0;
        private int bytesWrittenToBlock = 0;

        /**
         * Create a new output stream to the given DataNode.
         */
        public DFSOutputStream(UTF8 src, boolean overwrite) throws IOException {
            this.src = src;
            this.overwrite = overwrite;

            // zeng: 创建一个本地文件作为备份文件
            this.backupFile = newBackupFile();
            this.backupStream = new FileOutputStream(backupFile);
        }

        // zeng: 创建一个本地文件作为备份文件
        private File newBackupFile() throws IOException {

            // zeng: dfs data dir /  tmp / client-randomid
            File result = conf.getFile("dfs.data.dir",
                    "tmp" + File.separator +
                            "client-" + Math.abs(r.nextLong()));
            result.deleteOnExit();

            return result;
        }

        // zeng: next block outputstream

        /**
         * Open a DataOutputStream to a DataNode so that it can be written to.
         * This happens when a file is created and each time a new block is allocated.
         * Must get block ID and the IDs of the destinations from the namenode.
         */
        private synchronized void nextBlockOutputStream() throws IOException {
            boolean retry = false;
            long start = System.currentTimeMillis();
            do {
                retry = false;

                long localstart = System.currentTimeMillis();
                boolean blockComplete = false;
                LocatedBlock lb = null;

                while (!blockComplete) {    // zeng: 直到分配到
                    if (firstTime) {    // zeng: 第一个block
                        // zeng: 为新文件 选择DatanodeInfo 和 分配第一个block
                        lb = namenode.create(src.toString(), clientName.toString(), localName, overwrite);
                    } else {
                        // zeng: 为文件增加一个block 并为block选择DatanodeInfo
                        lb = namenode.addBlock(src.toString(), localName);
                    }

                    // zeng: 直到分配到
                    if (lb == null) {
                        try {
                            Thread.sleep(400);
                            if (System.currentTimeMillis() - localstart > 5000) {
                                LOG.info("Waiting to find new output block node for " + (System.currentTimeMillis() - start) + "ms");
                            }
                        } catch (InterruptedException ie) {
                        }
                    } else {
                        blockComplete = true;
                    }
                }

                // zeng: block
                block = lb.getBlock();
                // zeng: DatanodeInfo 数组
                DatanodeInfo nodes[] = lb.getLocations();

                //
                // Connect to first DataNode in the list.  Abort if this fails.
                //
                // zeng: 第一个Datanode
                InetSocketAddress target = DataNode.createSocketAddr(nodes[0].getName().toString());
                try {
                    // zeng: datanode socket
                    s = new Socket();
                    s.connect(target, READ_TIMEOUT);
                    s.setSoTimeout(READ_TIMEOUT);

                } catch (IOException ie) {
                    // Connection failed.  Let's wait a little bit and retry
                    try {
                        if (System.currentTimeMillis() - start > 5000) {
                            LOG.info("Waiting to find target node: " + target);
                        }
                        Thread.sleep(6000);
                    } catch (InterruptedException iex) {
                    }
                    if (firstTime) {
                        namenode.abandonFileInProgress(src.toString());
                    } else {
                        namenode.abandonBlock(block, src.toString());
                    }
                    retry = true;
                    continue;
                }

                //
                // Xmit header info to datanode
                //
                // zeng: socket outputstream
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()));

                // zeng: 发送操作码
                out.write(OP_WRITE_BLOCK);
                // zeng: 发送shouldReportBlock
                out.writeBoolean(false);
                // zeng: 发送block对象
                block.write(out);

                // zeng: 发送DatanodeInfo数组
                out.writeInt(nodes.length);
                for (int i = 0; i < nodes.length; i++) {
                    nodes[i].write(out);
                }

                // zeng: 发送输出编码
                out.write(CHUNKED_ENCODING);

                // zeng: 重置bytesWrittenToBlock
                bytesWrittenToBlock = 0;

                // zeng: 设置新block的outputstream
                blockStream = out;
                // zeng:  设置新block的inputstream
                blockReplyStream = new DataInputStream(new BufferedInputStream(s.getInputStream()));
            } while (retry);

            // zeng: 下一次
            firstTime = false;
        }

        /**
         * We're referring to the file pos here
         */
        public synchronized long getPos() throws IOException {
            return filePos;
        }

        /**
         * Writes the specified byte to this output stream.
         */
        public synchronized void write(int b) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if ((bytesWrittenToBlock + pos == BLOCK_SIZE) ||
                    (pos >= BUFFER_SIZE)) {
                flush();
            }
            outBuf[pos++] = (byte) b;
            filePos++;
        }

        /**
         * Writes the specified bytes to this output stream.
         */
        public synchronized void write(byte b[], int off, int len)
                throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            while (len > 0) {

                int remaining = BUFFER_SIZE - pos;

                int toWrite = Math.min(remaining, len);

                // zeng: 复制到outBuf
                System.arraycopy(b, off, outBuf, pos, toWrite);

                // zeng: 一些index
                pos += toWrite;
                off += toWrite;
                len -= toWrite;
                filePos += toWrite;

                // zeng: 如果outBuf已经满了 或者 本次block过程 所需的数据 已经达到, 执行flush
                if ((bytesWrittenToBlock + pos >= BLOCK_SIZE) || (pos == BUFFER_SIZE)) {
                    flush();
                }
            }
        }

        /**
         * Flush the buffer, getting a stream to a new block if necessary.
         */
        public synchronized void flush() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            if (bytesWrittenToBlock + pos >= BLOCK_SIZE) {  // zeng: 如果 本次block过程 所需的数据 已经达到
                flushData(BLOCK_SIZE - bytesWrittenToBlock);    // zeng: 本block过程中剩下要发送的数据
            }

            if (bytesWrittenToBlock == BLOCK_SIZE) {    // zeng: 本次block过程的数据 已经全部写入backup file
                endBlock();
            }

            // zeng: flush outBuf
            flushData(pos);
        }

        /**
         * Actually flush the accumulated bytes to the remote node,
         * but no more bytes than the indicated number.
         */
        private synchronized void flushData(int maxPos) throws IOException {
            // zeng: 这次flush中 要把outBuf什么位置之前 的字节发送出去
            int workingPos = Math.min(pos, maxPos);

            if (workingPos > 0) {
                //
                // To the local block backup, write just the bytes
                //
                // zeng: 只是写到backup file, endBlock时再统一从backup file读取发送
                backupStream.write(outBuf, 0, workingPos);

                //
                // Track position
                //
                // zeng: 这个 block 过程 累计写入多少字节到了backup file
                bytesWrittenToBlock += workingPos;

                // zeng: outBuf里剩下没写的字节 挪到 数组开头
                System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);

                // zeng: outBuf写到什么位置
                pos -= workingPos;
            }
        }

        // zeng: 发送block数据, 发送成功则通知namenode

        /**
         * We're done writing to the current block.
         */
        private synchronized void endBlock() throws IOException {
            //
            // Done with local copy
            //
            backupStream.close();

            //
            // Send it to datanode
            //
            boolean mustRecover = true;
            while (mustRecover) {
                // zeng: block outputstream
                nextBlockOutputStream();

                // zeng: backup file inputstream
                InputStream in = new FileInputStream(backupFile);
                try {
                    // zeng: 读到buf
                    byte buf[] = new byte[BUFFER_SIZE];
                    int bytesRead = in.read(buf);

                    while (bytesRead > 0) { // zeng: 直到读完backup file
                        // zeng: 发送buf len
                        blockStream.writeLong((long) bytesRead);
                        // zeng: 发送buf
                        blockStream.write(buf, 0, bytesRead);
                        // zeng: 读到buf
                        bytesRead = in.read(buf);
                    }

                    // zeng: 结束本次block
                    internalClose();

                    mustRecover = false;
                } catch (IOException ie) {
                    handleSocketException(ie);
                } finally {
                    in.close();
                }
            }

            // zeng: 清空backup file内容
            //
            // Delete local backup, start new one
            //
            backupFile.delete();
            backupFile = newBackupFile();
            backupStream = new FileOutputStream(backupFile);
            bytesWrittenToBlock = 0;
        }


        // zeng: 结束本次block

        /**
         * Close down stream to remote datanode.
         */
        private synchronized void internalClose() throws IOException {
            // zeng: 表示结束
            blockStream.writeLong(0);
            // zeng: flush
            blockStream.flush();

            // zeng: block是否上传完成
            long complete = blockReplyStream.readLong();

            // zeng: 报错
            if (complete != WRITE_COMPLETE) {
                LOG.info("Did not receive WRITE_COMPLETE flag: " + complete);
                throw new IOException("Did not receive WRITE_COMPLETE_FLAG: " + complete);
            }

            // zeng: 读取LocatedBlock(block对象 和 block已上传到哪些datablock)
            LocatedBlock lb = new LocatedBlock();
            lb.readFields(blockReplyStream);

            // zeng: 上报 LocatedBlock 给 namenode
            namenode.reportWrittenBlock(lb);

            s.close();
            s = null;
        }

        private void handleSocketException(IOException ie) throws IOException {
            LOG.log(Level.WARNING, "Error while writing.", ie);
            try {
                if (s != null) {
                    s.close();
                    s = null;
                }
            } catch (IOException ie2) {
                LOG.log(Level.WARNING, "Error closing socket.", ie2);
            }
            namenode.abandonBlock(block, src.toString());
        }

        /**
         * Closes this output stream and releases any system
         * resources associated with this stream.
         */
        public synchronized void close() throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }

            // zeng: 虽然没有达到block大小, 但是文件已经读完了, 所以也要结束本次block过程
            flush();
            if (filePos == 0 || bytesWrittenToBlock != 0) {
                try {
                    endBlock();
                } catch (IOException e) {
                    namenode.abandonFileInProgress(src.toString());
                    throw e;
                }
            }

            backupStream.close();
            backupFile.delete();

            if (s != null) {
                s.close();
                s = null;
            }
            super.close();

            long localstart = System.currentTimeMillis();
            boolean fileComplete = false;
            while (!fileComplete) { // zeng: 直到加入成功
                // zeng: 设置block的len, 将文件加入文件树
                fileComplete = namenode.complete(src.toString(), clientName.toString());

                if (!fileComplete) {
                    try {
                        Thread.sleep(400);
                        if (System.currentTimeMillis() - localstart > 5000) {
                            LOG.info("Could not complete file, retrying...");
                        }
                    } catch (InterruptedException ie) {
                    }
                }
            }

            closed = true;
        }
    }
}
