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

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

// zeng: 管理 block 数据, 每个block数据都是 本地文件系统中的一个文件

/**************************************************
 * FSDataset manages a set of data blocks.  Each block
 * has a unique name and an extent on disk.
 *
 * @author Mike Cafarella
 ***************************************************/
class FSDataset implements FSConstants {
    static final double USABLE_DISK_PCT = 0.98;

    /**
     * A node type that can be built into a tree reflecting the
     * hierarchy of blocks on the local disk.
     */
    class FSDir {
        File dir;
        FSDir children[];

        /**
         *
         */
        public FSDir(File dir) {
            this.dir = dir;
            this.children = null;
        }

        /**
         *
         */
        public File getDirName() {
            return dir;
        }

        /**
         *
         */
        public FSDir[] getChildren() {
            return children;
        }

        // zeng: 加入block文件树中
        /**
         *
         */
        public void addBlock(Block b, File src) {
            addBlock(b, src, b.getBlockId(), 0);
        }

        // zeng: 加入block文件树中
        /**
         *
         */
        void addBlock(Block b, File src, long blkid, int depth) {
            //
            // Add to the local dir, if no child dirs
            //
            if (children == null) {
                // zeng: 把 tmp/block 文件 移动为 data/block
                src.renameTo(new File(dir, b.getBlockName()));

                // zeng: TODO 用来把block文件存储到data的子目录下 免得一级目录太大?
                //
                // Test whether this dir's contents should be busted 
                // up into subdirs.
                //

                // REMIND - mjc - sometime soon, we'll want this code
                // working.  It prevents the datablocks from all going
                // into a single huge directory.
                /**
                 File localFiles[] = dir.listFiles();
                 if (localFiles.length == 16) {
                 //
                 // Create all the necessary subdirs
                 //
                 this.children = new FSDir[16];
                 for (int i = 0; i < children.length; i++) {
                 String str = Integer.toBinaryString(i);
                 try {
                 File subdir = new File(dir, "dir_" + str);
                 subdir.mkdir();
                 children[i] = new FSDir(subdir);
                 } catch (StringIndexOutOfBoundsException excep) {
                 excep.printStackTrace();
                 System.out.println("Ran into problem when i == " + i + " an str = " + str);
                 }
                 }

                 //
                 // Move existing files into new dirs
                 //
                 for (int i = 0; i < localFiles.length; i++) {
                 Block srcB = new Block(localFiles[i]);
                 File dst = getBlockFilename(srcB, blkid, depth);
                 if (!src.renameTo(dst)) {
                 System.out.println("Unexpected problem in renaming " + src);
                 }
                 }
                 }
                 **/
            } else {
                // Find subdir
                children[getHalfByte(blkid, depth)].addBlock(b, src, blkid, depth + 1);
            }

        }

        // zeng: 获取这个datanode下所有的block
        /**
         * Fill in the given blockSet with any child blocks
         * found at this node.
         */
        public void getBlockInfo(TreeSet blockSet) {
            if (children != null) {
                for (int i = 0; i < children.length; i++) {
                    children[i].getBlockInfo(blockSet);
                }
            }

            // zeng: ls
            File blockFiles[] = dir.listFiles();

            for (int i = 0; i < blockFiles.length; i++) {
                if (Block.isBlockFilename(blockFiles[i])) {
                    // zeng: block对象
                    blockSet.add(new Block(blockFiles[i], blockFiles[i].length()));
                }
            }
        }

        // zeng: block数据本地文件

        /**
         * Find the file that corresponds to the given Block
         */
        public File getBlockFilename(Block b) {
            return getBlockFilename(b, b.getBlockId(), 0);
        }

        /**
         * Helper method to find file for a Block
         */
        private File getBlockFilename(Block b, long blkid, int depth) {
            if (children == null) {
                // zeng: block数据本地文件
                return new File(dir, b.getBlockName());
            } else {
                // 
                // Lift the 4 bits starting at depth, going left->right.
                // That means there are 2^4 possible children, or 16.
                // The max depth is thus ((len(long) / 4) == 16).
                //
                // zeng: TODO
                return children[getHalfByte(blkid, depth)].getBlockFilename(b, blkid, depth + 1);
            }
        }

        /**
         * Returns a number 0-15, inclusive.  Pulls out the right
         * half-byte from the indicated long.
         */
        private int getHalfByte(long blkid, int halfByteIndex) {
            blkid = blkid >> ((15 - halfByteIndex) * 4);
            return (int) ((0x000000000000000F) & blkid);
        }

        public String toString() {
            return "FSDir{" +
                    "dir=" + dir +
                    ", children=" + (children == null ? null : Arrays.asList(children)) +
                    "}";
        }
    }

    //////////////////////////////////////////////////////
    //
    // FSDataSet
    //
    //////////////////////////////////////////////////////

    DF diskUsage;
    File data = null, tmp = null;
    long reserved = 0;
    FSDir dirTree;
    TreeSet ongoingCreates = new TreeSet();

    /**
     * An FSDataset has a directory where it loads its data files.
     */
    public FSDataset(File dir, Configuration conf) throws IOException {
        // zeng: df命令获取到的硬盘信息
        diskUsage = new DF(dir.getCanonicalPath(), conf);

        // zeng: dir/data 目录
        this.data = new File(dir, "data");

        // zeng: 创建目录
        if (!data.exists()) {
            data.mkdirs();
        }

        // zeng: 清空 dir/tmp目录
        this.tmp = new File(dir, "tmp");
        if (tmp.exists()) {
            FileUtil.fullyDelete(tmp, conf);
        }
        this.tmp.mkdirs();

        // zeng: block文件树
        this.dirTree = new FSDir(data);
    }

    /**
     * Return total capacity, used and unused
     */
    public long getCapacity() throws IOException {
        return diskUsage.getCapacity();
    }

    // zeng: 还剩多少硬盘空间可用
    /**
     * Return how many bytes can still be stored in the FSDataset
     */
    public long getRemaining() throws IOException {
        return ((long) Math.round(USABLE_DISK_PCT * diskUsage.getAvailable())) - reserved;
    }

    // zeng: 获取block len
    /**
     * Find the block's on-disk length
     */
    public long getLength(Block b) throws IOException {
        if (!isValidBlock(b)) {
            throw new IOException("Block " + b + " is not valid.");
        }
        File f = getFile(b);

        return f.length();
    }

    // zeng: block file inputstream
    /**
     * Get a stream of data from the indicated block.
     */
    public InputStream getBlockData(Block b) throws IOException {
        if (!isValidBlock(b)) {
            throw new IOException("Block " + b + " is not valid.");
        }
        return new FileInputStream(getFile(b));
    }

    /**
     * A Block b will be coming soon!
     */
    public boolean startBlock(Block b) throws IOException {
        //
        // Make sure the block isn't 'valid'
        //
        if (isValidBlock(b)) {
            throw new IOException("Block " + b + " is valid, and cannot be created.");
        }
        return true;
    }

    /**
     * Start writing to a block file
     */
    public OutputStream writeToBlock(Block b) throws IOException {
        //
        // Make sure the block isn't a valid one - we're still creating it!
        //
        // zeng: block数据文件 是否存在
        if (isValidBlock(b)) {
            throw new IOException("Block " + b + " is valid, and cannot be written to.");
        }

        //
        // Serialize access to /tmp, and check if file already there.
        //
        File f = null;
        synchronized (ongoingCreates) {
            //
            // Is it already in the create process?
            //
            if (ongoingCreates.contains(b)) {   // zeng: block是否已经在写了
                throw new IOException("Block " + b + " has already been started (though not completed), and thus cannot be created.");
            }

            //
            // Check if we have too little space
            //
            if (getRemaining() < BLOCK_SIZE) {  // zeng: 剩余空间是否足够
                throw new IOException("Insufficient space for an additional block");
            }

            //
            // OK, all's well.  Register the create, adjust 
            // 'reserved' size, & create file
            //
            ongoingCreates.add(b);  // zeng: 正在写
            reserved += BLOCK_SIZE; // zeng: 这部分空间已经分配出去了

            // zeng: dir/tmp/blockname
            f = getTmpFile(b);

            try {
                if (f.exists()) {
                    throw new IOException("Unexpected problem in startBlock() for " + b + ".  File " + f + " should not be present, but is.");
                }

                //
                // Create the zero-length temp file
                //
                if (!f.createNewFile()) {   // zeng: 创建tmp文件
                    throw new IOException("Unexpected problem in startBlock() for " + b + ".  File " + f + " should be creatable, but is already present.");
                }
            } catch (IOException ie) {
                System.out.println("Exception!  " + ie);
                ongoingCreates.remove(b);
                reserved -= BLOCK_SIZE;
                throw ie;
            }
        }

        //
        // Finally, allow a writer to the block file
        // REMIND - mjc - make this a filter stream that enforces a max
        // block size, so clients can't go crazy
        //
        // zeng: tmp文件outputstream
        return new FileOutputStream(f);
    }

    //
    // REMIND - mjc - eventually we should have a timeout system
    // in place to clean up block files left by abandoned clients.
    // We should have some timer in place, so that if a blockfile
    // is created but non-valid, and has been idle for >48 hours,
    // we can GC it safely.
    //

    // zeng: block写入完毕
    /**
     * Complete the block write!
     */
    public void finalizeBlock(Block b) throws IOException {
        // zeng: 写好的tmp文件
        File f = getTmpFile(b);

        if (!f.exists()) {
            throw new IOException("No temporary file " + f + " for block " + b);
        }

        synchronized (ongoingCreates) {
            //
            // Make sure still registered as ongoing
            //
            if (!ongoingCreates.contains(b)) {
                throw new IOException("Tried to finalize block " + b + ", but not in ongoingCreates table");
            }

            // zeng: 设置文件大小到block对象的len中
            long finalLen = f.length();
            b.setNumBytes(finalLen);

            //
            // Move the file
            // (REMIND - mjc - shame to move the file within a synch
            // section!  Maybe remove this?)
            //
            // zeng: 加入block文件树中
            dirTree.addBlock(b, f);

            //
            // Done, so deregister from ongoingCreates
            //
            if (!ongoingCreates.remove(b)) {    // zeng: block写完了
                throw new IOException("Tried to finalize block " + b + ", but could not find it in ongoingCreates after file-move!");
            }

            // zeng: 保留的空间已经写入了
            reserved -= BLOCK_SIZE;
        }
    }

    // zeng: 获取这个datanode下所有block

    /**
     * Return a table of block data
     */
    public Block[] getBlockReport() {
        TreeSet blockSet = new TreeSet();
        dirTree.getBlockInfo(blockSet);

        Block blockTable[] = new Block[blockSet.size()];
        int i = 0;
        for (Iterator it = blockSet.iterator(); it.hasNext(); i++) {
            blockTable[i] = (Block) it.next();
        }
        return blockTable;
    }

    // zeng: block数据文件 是否存在

    /**
     * Check whether the given block is a valid one.
     */
    public boolean isValidBlock(Block b) {
        File f = getFile(b);
        if (f.exists()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * We're informed that a block is no longer valid.  We
     * could lazily garbage-collect the block, but why bother?
     * just get rid of it.
     */
    public void invalidate(Block invalidBlks[]) throws IOException {
        for (int i = 0; i < invalidBlks.length; i++) {
            // zeng: block file
            File f = getFile(invalidBlks[i]);

            // long len = f.length();

            // zeng: 删除block file
            if (!f.delete()) {
                throw new IOException("Unexpected error trying to delete block " + invalidBlks[i] + " at file " + f);
            }
        }
    }

    //  zeng: get file by block object
    /**
     * Turn the block identifier into a filename.
     */
    File getFile(Block b) {
        // REMIND - mjc - should cache this result for performance
        return dirTree.getBlockFilename(b);
    }

    /**
     * Get the temp file, if this block is still being created.
     */
    File getTmpFile(Block b) {
        // zeng: dir/tmp/blockname
        // REMIND - mjc - should cache this result for performance
        return new File(tmp, b.getBlockName());
    }

    public String toString() {
        return "FSDataset{" +
                "dirpath='" + diskUsage.getDirPath() + "'" +
                "}";
    }

}
