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

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;

/*************************************************
 * FSDirectory stores the filesystem directory state.
 * It handles writing/loading values to disk, and logging
 * changes as we go.
 *
 * It keeps the filename->blockset mapping always-current
 * and logged to disk.
 *
 * @author Mike Cafarella
 *************************************************/
class FSDirectory implements FSConstants {
    static String FS_IMAGE = "fsimage";
    static String NEW_FS_IMAGE = "fsimage.new";
    static String OLD_FS_IMAGE = "fsimage.old";

    private static final byte OP_ADD = 0;
    private static final byte OP_RENAME = 1;
    private static final byte OP_DELETE = 2;
    private static final byte OP_MKDIR = 3;

    // zeng: 文件用inode表示, 包括name blocks parent children  这些元素

    /******************************************************
     * We keep an in-memory representation of the file/block
     * hierarchy.
     ******************************************************/
    class INode {
        public String name;
        public INode parent;
        public TreeMap children = new TreeMap();
        public Block blocks[];

        /**
         *
         */
        INode(String name, INode parent, Block blocks[]) {
            this.name = name;
            this.parent = parent;
            this.blocks = blocks;
        }

        // zeng: inode是否目录

        /**
         * Check whether it's a directory
         *
         * @return
         */
        synchronized public boolean isDir() {
            return (blocks == null);
        }

        // zeng: 根据 路径字符串 从 文件树 中获取相应inode

        /**
         * This is the external interface
         */
        INode getNode(String target) {
            if (!target.startsWith("/") || target.length() == 0) { // zeng: 不合法路径
                return null;
            } else if (parent == null && "/".equals(target)) {  // zeng: 根目录
                return this;
            } else {
                Vector components = new Vector();

                // zeng: 每1级的目录名(最后一级是文件名)
                int start = 0;
                int slashid = 0;
                while (start < target.length() && (slashid = target.indexOf('/', start)) >= 0) {
                    components.add(target.substring(start, slashid));
                    start = slashid + 1;
                }

                if (start < target.length()) {
                    components.add(target.substring(start));
                }

                return getNode(components, 0);
            }
        }

        /**
         *
         */
        INode getNode(Vector components, int index) {
            if (!name.equals((String) components.elementAt(index))) {
                return null;
            }

            // zeng: 本inode就是最后一级
            if (index == components.size() - 1) {
                return this;
            }

            // zeng: 从根目录一级一级往下查询children
            // Check with children
            INode child = (INode) children.get(components.elementAt(index + 1));
            if (child == null) {
                return null;
            } else {
                return child.getNode(components, index + 1);
            }
        }

        // zeng: 加入一个inode

        /**
         *
         */
        INode addNode(String target, Block blks[]) {
            if (getNode(target) != null) {  // zeng: 是否已存在
                return null;
            } else {
                String parentName = DFSFile.getDFSParent(target);
                if (parentName == null) {
                    return null;
                }

                // zeng: parent node
                INode parentNode = getNode(parentName);

                if (parentNode == null) {
                    return null;
                } else {
                    // zeng: 文件名
                    String targetName = new File(target).getName();

                    // zeng: 新建一个inode
                    INode newItem = new INode(targetName, parentNode, blks);

                    // zeng: 加入children map中
                    parentNode.children.put(targetName, newItem);

                    return newItem;
                }
            }
        }

        // zeng: 从文件树中移除本node

        /**
         *
         */
        boolean removeNode() {
            if (parent == null) {
                return false;
            } else {
                parent.children.remove(name);
                return true;
            }
        }

        // zeng: 这棵子文件树下的所有的blocks加入 vector中

        /**
         * Collect all the blocks at this INode and all its children.
         * This operation is performed after a node is removed from the tree,
         * and we want to GC all the blocks at this node and below.
         */
        void collectSubtreeBlocks(Vector v) {
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    v.add(blocks[i]);
                }
            }
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                child.collectSubtreeBlocks(v);
            }
        }

        // zeng: 遍历文件树,统计文件数

        /**
         *
         */
        int numItemsInTree() {
            int total = 0;
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.numItemsInTree();
            }
            return total + 1;
        }

        // zeng: 获取节点全描述符

        /**
         *
         */
        String computeName() {
            if (parent != null) {
                return parent.computeName() + "/" + name;
            } else {
                return name;
            }
        }

        /**
         *
         */
        long computeFileLength() {
            long total = 0;
            if (blocks != null) {
                for (int i = 0; i < blocks.length; i++) {
                    total += blocks[i].getNumBytes();
                }
            }
            return total;
        }

        /**
         *
         */
        long computeContentsLength() {
            long total = computeFileLength();
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                total += child.computeContentsLength();
            }
            return total;
        }

        /**
         *
         */
        void listContents(Vector v) {
            if (parent != null && blocks != null) {
                v.add(this);
            }

            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                v.add(child);
            }
        }

        // zeng: 文件树写入out

        /**
         *
         */
        void saveImage(String parentPrefix, DataOutputStream out) throws IOException {
            String fullName = "";
            if (parent != null) {
                // zeng: 写入当前节点的全描述符
                fullName = parentPrefix + "/" + name;
                new UTF8(fullName).write(out);

                // zeng: 写入 当前节点的block数 及 所有block对象
                if (blocks == null) {
                    out.writeInt(0);
                } else {
                    out.writeInt(blocks.length);

                    // zeng: 当前节点的所有block对象
                    for (int i = 0; i < blocks.length; i++) {
                        blocks[i].write(out);
                    }
                }
            }

            // zeng: children中所有节点递归调用saveImage
            for (Iterator it = children.values().iterator(); it.hasNext(); ) {
                INode child = (INode) it.next();
                child.saveImage(fullName, out);
            }

        }

    }

    // zeng: 根目录
    INode rootDir = new INode("", null, null);

    TreeSet activeBlocks = new TreeSet();
    TreeMap activeLocks = new TreeMap();
    DataOutputStream editlog = null;
    boolean ready = false;

    /**
     * Access an existing dfs name directory.
     */
    public FSDirectory(File dir) throws IOException {
        // zeng: image文件
        File fullimage = new File(dir, "image");

        if (!fullimage.exists()) {
            throw new IOException("NameNode not formatted: " + dir);
        }

        // zeng: edits文件
        File edits = new File(dir, "edits");

        if (loadFSImage(fullimage, edits)) {    // zeng: 读取image和edits文件,以恢复文件树
            // zeng: 如果有edit记录, 那么更新image文件
            saveFSImage(fullimage, edits);
        }

        synchronized (this) {
            this.ready = true;
            this.notifyAll();

            // zeng: edits文件 outputstream
            this.editlog = new DataOutputStream(new FileOutputStream(edits));
        }
    }

    /**
     * Create a new dfs name directory.  Caution: this destroys all files
     * in this filesystem.
     */
    public static void format(File dir, Configuration conf)
            throws IOException {
        File image = new File(dir, "image");
        File edits = new File(dir, "edits");

        if (!((!image.exists() || FileUtil.fullyDelete(image, conf)) &&
                (!edits.exists() || edits.delete()) &&
                image.mkdirs())) {

            throw new IOException("Unable to format: " + dir);
        }
    }

    /**
     * Shutdown the filestore
     */
    public void close() throws IOException {
        editlog.close();
    }

    /**
     * Block until the object is ready to be used.
     */
    void waitForReady() {
        if (!ready) {
            synchronized (this) {
                while (!ready) {
                    try {
                        this.wait(5000);
                    } catch (InterruptedException ie) {
                    }
                }
            }
        }
    }

    // zeng: 读取image和edits文件,以恢复文件树

    /**
     * Load in the filesystem image.  It's a big list of
     * filenames and blocks.  Return whether we should
     * "re-save" and consolidate the edit-logs
     */
    boolean loadFSImage(File fsdir, File edits) throws IOException {
        //
        // Atomic move sequence, to recover from interrupted save

        //  zeng: image/fsimage
        File curFile = new File(fsdir, FS_IMAGE);
        // zeng: image/fsimage.new
        File newFile = new File(fsdir, NEW_FS_IMAGE);
        // zeng: image/fsimage.old
        File oldFile = new File(fsdir, OLD_FS_IMAGE);

        // zeng: 合并image和edits时宕机的容错处理
        // Maybe we were interrupted between 2 and 4
        if (oldFile.exists() && curFile.exists()) {
            oldFile.delete();
            if (edits.exists()) {
                edits.delete();
            }
        } else if (oldFile.exists() && newFile.exists()) {
            // Or maybe between 1 and 2
            newFile.renameTo(curFile);
            oldFile.delete();
        } else if (curFile.exists() && newFile.exists()) {
            // Or else before stage 1, in which case we lose the edits
            newFile.delete();
        }

        //
        // Load in bits
        //
        // zeng: 先从image中恢复文件树
        if (curFile.exists()) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));

            try {
                // zeng: 文件数(包括目录)
                int numFiles = in.readInt();

                for (int i = 0; i < numFiles; i++) {    // zeng: 所有文件加入文件树
                    // zeng: 文件全描述符
                    UTF8 name = new UTF8();
                    name.readFields(in);

                    // zeng: block数
                    int numBlocks = in.readInt();

                    if (numBlocks == 0) {
                        // zeng: 加入文件树
                        unprotectedAddFile(name, null);
                    } else {
                        Block blocks[] = new Block[numBlocks];
                        for (int j = 0; j < numBlocks; j++) {
                            // zeng: block对象
                            blocks[j] = new Block();
                            // zeng: block对象字段
                            blocks[j].readFields(in);
                        }

                        // zeng: 加入文件树
                        unprotectedAddFile(name, blocks);
                    }
                }
            } finally {
                in.close();
            }
        }

        if (edits.exists() && loadFSEdits(edits) > 0) { // zeng: 将edits中记录的操作全部应用到文件树上
            return true;
        } else {
            return false;
        }
    }

    /**
     * Load an edit log, and apply the changes to the in-memory structure
     * <p>
     * This is where we apply edits that we've been writing to disk all
     * along.
     */
    int loadFSEdits(File edits) throws IOException {
        int numEdits = 0;

        if (edits.exists()) {
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(edits)));

            try {
                while (in.available() > 0) {
                    // zeng: 读取 op code
                    byte opcode = in.readByte();

                    // zeng: 多少条edit记录
                    numEdits++;

                    switch (opcode) {
                        case OP_ADD: {
                            // zeng: 文件全描述符
                            UTF8 name = new UTF8();
                            name.readFields(in);

                            // zeng: 所有block对象
                            ArrayWritable aw = new ArrayWritable(Block.class);
                            aw.readFields(in);

                            // zeng: 构建blocks数组
                            Writable writables[] = (Writable[]) aw.get();
                            Block blocks[] = new Block[writables.length];
                            System.arraycopy(writables, 0, blocks, 0, blocks.length);

                            // zeng: 加入文件树
                            unprotectedAddFile(name, blocks);

                            break;
                        }
                        case OP_RENAME: {
                            // zeng: 读取 源文件全描述符 和 目标文件全描述符名
                            UTF8 src = new UTF8();
                            UTF8 dst = new UTF8();
                            src.readFields(in);
                            dst.readFields(in);

                            // zeng: 重命名
                            unprotectedRenameTo(src, dst);

                            break;
                        }
                        case OP_DELETE: {
                            // zeng: 读取文件全描述符
                            UTF8 src = new UTF8();
                            src.readFields(in);

                            // zeng: 从文件树中移除
                            unprotectedDelete(src);

                            break;
                        }
                        case OP_MKDIR: {
                            // zeng: 读取目录全描述符
                            UTF8 src = new UTF8();
                            src.readFields(in);

                            // zeng: 在文件树中加入 该目录
                            unprotectedMkdir(src.toString());
                            break;
                        }
                        default: {
                            throw new IOException("Never seen opcode " + opcode);
                        }
                    }
                }
            } finally {
                in.close();
            }
        }

        return numEdits;
    }

    // zeng: 用当前文件树更新image文件

    /**
     * Save the contents of the FS image
     */
    void saveFSImage(File fullimage, File edits) throws IOException {
        File curFile = new File(fullimage, FS_IMAGE);
        File newFile = new File(fullimage, NEW_FS_IMAGE);
        File oldFile = new File(fullimage, OLD_FS_IMAGE);

        //
        // Write out data
        //
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(newFile)));
        try {
            // zeng: 写入文件数
            out.writeInt(rootDir.numItemsInTree() - 1);
            // zeng: 文件树写入fsimage.new
            rootDir.saveImage("", out);
        } finally {
            out.close();
        }

        // zeng:  以下操作用来将 fsimage.new 替换掉 fsimage

        //
        // Atomic move sequence
        //
        // 1.  Move cur to old
        curFile.renameTo(oldFile);

        // 2.  Move new to cur
        newFile.renameTo(curFile);

        // 3.  Remove pending-edits file (it's been integrated with newFile)
        edits.delete();

        // 4.  Delete old
        oldFile.delete();
    }

    // zeng: 对文件树的操作写入edits文件

    /**
     * Write an operation to the edit log
     */
    void logEdit(byte op, Writable w1, Writable w2) {
        synchronized (editlog) {
            try {
                // zeng: 写入opcode
                editlog.write(op);

                // zeng: 写入节点全描述符
                if (w1 != null) {
                    w1.write(editlog);
                }

                // zeng: 写入block对象数组
                if (w2 != null) {
                    w2.write(editlog);
                }
            } catch (IOException ie) {
            }
        }
    }

    /**
     * Add the given filename to the fs.
     */
    public boolean addFile(UTF8 src, Block blocks[]) {
        waitForReady();

        // Always do an implicit mkdirs for parent directory tree
        mkdirs(DFSFile.getDFSParent(src.toString()));
        if (unprotectedAddFile(src, blocks)) {
            logEdit(OP_ADD, src, new ArrayWritable(Block.class, blocks));
            return true;
        } else {
            return false;
        }
    }

    // zeng: 文件加入文件树

    /**
     *
     */
    boolean unprotectedAddFile(UTF8 name, Block blocks[]) {
        synchronized (rootDir) {
            if (blocks != null) {
                // Add file->block mapping
                // zeng: 所有block对象加入activeBlocks set中
                for (int i = 0; i < blocks.length; i++) {
                    activeBlocks.add(blocks[i]);
                }
            }

            // zeng: 加入文件树
            return (rootDir.addNode(name.toString(), blocks) != null);
        }
    }

    /**
     * Change the filename
     */
    public boolean renameTo(UTF8 src, UTF8 dst) {
        waitForReady();
        if (unprotectedRenameTo(src, dst)) {
            logEdit(OP_RENAME, src, dst);
            return true;
        } else {
            return false;
        }
    }

    // zeng: 重命名(实际为移除旧的inode, 加入新的inode)

    /**
     *
     */
    boolean unprotectedRenameTo(UTF8 src, UTF8 dst) {
        synchronized (rootDir) {
            // zeng: 源文件全描述符对应inode
            INode removedNode = rootDir.getNode(src.toString());

            if (removedNode == null) {
                return false;
            }

            // zeng: 从文件树中移除本node
            removedNode.removeNode();

            // zeng: 如果目标文件全描述符是个目录, 那么要追加文件名
            if (isDir(dst)) {
                dst = new UTF8(dst.toString() + "/" + new File(src.toString()).getName());
            }

            // zeng: 目标inode 加入 文件树
            INode newNode = rootDir.addNode(dst.toString(), removedNode.blocks);

            if (newNode != null) {
                // zeng: 复制children
                newNode.children = removedNode.children;

                // zeng: 更新children中所有inode的parent
                for (Iterator it = newNode.children.values().iterator(); it.hasNext(); ) {
                    INode child = (INode) it.next();
                    child.parent = newNode;
                }
                return true;
            } else {    // zeng: 失败了, 还原原来的inode
                rootDir.addNode(src.toString(), removedNode.blocks);
                return false;
            }
        }
    }

    // zeng: 从文件树中移除
    /**
     * Remove the file from management, return blocks
     */
    public Block[] delete(UTF8 src) {
        // zeng: 等待文件树加载完成
        waitForReady();
        // zeng: 操作计入edits中
        logEdit(OP_DELETE, src, null);
        // zeng: 从文件树中移除
        return unprotectedDelete(src);
    }

    // zeng: 从文件树中移除

    /**
     *
     */
    Block[] unprotectedDelete(UTF8 src) {
        synchronized (rootDir) {
            // zeng: 获取inode
            INode targetNode = rootDir.getNode(src.toString());

            if (targetNode == null) {
                return null;
            } else {
                //
                // Remove the node from the namespace and GC all
                // the blocks underneath the node.
                //
                if (!targetNode.removeNode()) { // zeng: 从文件树中移除本inode
                    return null;
                } else {
                    // zeng: 这棵子文件树下的所有的blocks加入 vector中
                    Vector v = new Vector();
                    targetNode.collectSubtreeBlocks(v);

                    // zeng: 从activeBlocks set中移除
                    for (Iterator it = v.iterator(); it.hasNext(); ) {
                        Block b = (Block) it.next();
                        activeBlocks.remove(b);
                    }

                    // zeng: 所有移除的block
                    return (Block[]) v.toArray(new Block[v.size()]);
                }
            }
        }
    }

    /**
     *
     */
    public int obtainLock(UTF8 src, UTF8 holder, boolean exclusive) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders == null) {
            holders = new TreeSet();
            activeLocks.put(src, holders);
        }
        if (exclusive && holders.size() > 0) {
            return STILL_WAITING;
        } else {
            holders.add(holder);
            return COMPLETE_SUCCESS;
        }
    }

    /**
     *
     */
    public int releaseLock(UTF8 src, UTF8 holder) {
        TreeSet holders = (TreeSet) activeLocks.get(src);
        if (holders != null && holders.contains(holder)) {
            holders.remove(holder);
            if (holders.size() == 0) {
                activeLocks.remove(src);
            }
            return COMPLETE_SUCCESS;
        } else {
            return OPERATION_FAILED;
        }
    }

    /**
     * Get a listing of files given path 'src'
     * <p>
     * This function is admittedly very inefficient right now.  We'll
     * make it better later.
     */
    public DFSFileInfo[] getListing(UTF8 src) {
        String srcs = normalizePath(src);

        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(srcs);
            if (targetNode == null) {
                return null;
            } else {
                Vector contents = new Vector();
                targetNode.listContents(contents);

                DFSFileInfo listing[] = new DFSFileInfo[contents.size()];
                int i = 0;
                for (Iterator it = contents.iterator(); it.hasNext(); i++) {
                    listing[i] = new DFSFileInfo((INode) it.next());
                }
                return listing;
            }
        }
    }

    /**
     * Get the blocks associated with the file
     */
    public Block[] getFile(UTF8 src) {
        waitForReady();
        synchronized (rootDir) {
            INode targetNode = rootDir.getNode(src.toString());
            if (targetNode == null) {
                return null;
            } else {
                return targetNode.blocks;
            }
        }
    }

    /**
     * Check whether the filepath could be created
     */
    public boolean isValidToCreate(UTF8 src) {
        String srcs = normalizePath(src);
        synchronized (rootDir) {
            if (srcs.startsWith("/") &&
                    !srcs.endsWith("/") &&
                    rootDir.getNode(srcs) == null   // zeng: 是否已存在
            ) {
                return true;
            } else {
                return false;
            }
        }
    }

    // zeng: 文件全描述符是否目录

    /**
     * Check whether the path specifies a directory
     */
    public boolean isDir(UTF8 src) {
        synchronized (rootDir) {
            // zeng: inode
            INode node = rootDir.getNode(normalizePath(src));
            // zeng: inode是否目录
            return node != null && node.isDir();
        }
    }

    // zeng: 创建目录及其顶层目录

    /**
     * Create the given directory and all its parent dirs.
     */
    public boolean mkdirs(UTF8 src) {
        return mkdirs(src.toString());
    }

    /**
     * Create directory entries for every item
     */
    boolean mkdirs(String src) {
        src = normalizePath(new UTF8(src));

        // Use this to collect all the dirs we need to construct
        Vector v = new Vector();

        // zeng: 目录
        // The dir itself
        v.add(src);

        // zeng: 所有顶层目录
        // All its parents
        String parent = DFSFile.getDFSParent(src);
        while (parent != null) {
            v.add(parent);

            parent = DFSFile.getDFSParent(parent);
        }

        // Now go backwards through list of dirs, creating along
        // the way
        boolean lastSuccess = false;
        int numElts = v.size();
        for (int i = numElts - 1; i >= 0; i--) {    // zeng: 从最顶层目录开始创建
            String cur = (String) v.elementAt(i);

            // zeng:  在文件树中加入 该文件(目录), 返回创建的inode
            INode inserted = unprotectedMkdir(cur);

            if (inserted != null) {
                // zeng: 对文件树的操作写入edits文件
                logEdit(OP_MKDIR, new UTF8(inserted.computeName()), null);

                lastSuccess = true;
            } else {
                lastSuccess = false;
            }
        }
        return lastSuccess;
    }

    /**
     *
     */
    // zeng: 在文件树中加入 该目录, 返回创建的inode
    INode unprotectedMkdir(String src) {
        synchronized (rootDir) {
            return rootDir.addNode(src, null);
        }
    }

    /**
     *
     */
    String normalizePath(UTF8 src) {
        String srcs = src.toString();
        if (srcs.length() > 1 && srcs.endsWith("/")) {
            srcs = srcs.substring(0, srcs.length() - 1);
        }
        return srcs;
    }

    /**
     * Returns whether the given block is one pointed-to by a file.
     */
    public boolean isValidBlock(Block b) {
        synchronized (rootDir) {
            if (activeBlocks.contains(b)) {
                return true;
            } else {
                return false;
            }
        }
    }
}
