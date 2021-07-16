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
package org.apache.hadoop.fs;

import java.io.*;
import java.util.zip.Checksum;
import java.util.zip.CRC32;

import org.apache.hadoop.conf.Configuration;

/**
 * Utility that wraps a {@link FSOutputStream} in a {@link DataOutputStream},
 * buffers output through a {@link BufferedOutputStream} and creates a checksum
 * file.
 */
public class FSDataOutputStream extends DataOutputStream {
    public static final byte[] CHECKSUM_VERSION = new byte[]{'c', 'r', 'c', 0};

    /**
     * Store checksums for data.
     */
    private static class Summer extends FilterOutputStream {

        private FSDataOutputStream sums;
        private Checksum sum = new CRC32();
        private int inSum;
        private int bytesPerSum;

        public Summer(FileSystem fs, File file, boolean overwrite, Configuration conf)
                throws IOException {
            super(fs.createRaw(file, overwrite)); // zeng: dfs file outputstream (DFSOutputStream)

            this.bytesPerSum = conf.getInt("io.bytes.per.checksum", 512);
            // zeng: dfs sum file outputstream(DFSOutputStream)
            this.sums = new FSDataOutputStream(fs.createRaw(fs.getChecksumFile(file), true), conf);
            // zeng: magic num + version
            sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
            // zeng: 多少byte 对应一个 checksum
            sums.writeInt(this.bytesPerSum);
        }

        public void write(byte b[], int off, int len) throws IOException {
            int summed = 0;

            while (summed < len) {  // zeng: 本buff
                // zeng: 本checksum所对应byte还有多少要收集
                int goal = this.bytesPerSum - inSum;

                // zeng: 本buff里还有多少未处理
                int inBuf = len - summed;

                // zeng: 本次处理多少byte
                int toSum = inBuf <= goal ? inBuf : goal;

                // zeng: 本次处理的byte
                sum.update(b, off + summed, toSum);

                // zeng: 本buff处理了多少
                summed += toSum;
                // zeng: 本checksum所对应byte已经收集了多少
                inSum += toSum;

                // zeng: 本checksum所对应byte已经收集完毕
                if (inSum == this.bytesPerSum) {
                    writeSum();
                }
            }

            // zeng: DFSOutputStream.write
            out.write(b, off, len);
        }

        private void writeSum() throws IOException {
            if (inSum != 0) {
                // zeng: 发送checksum
                sums.writeInt((int) sum.getValue());
                // zeng: 新的checksum
                sum.reset();
                // zeng: 重置
                inSum = 0;
            }
        }

        public void close() throws IOException {
            writeSum();
            sums.close();

            // zeng: DFSOutputStream.close
            super.close();
        }

    }

    private static class PositionCache extends FilterOutputStream {
        long position;

        public PositionCache(OutputStream out) throws IOException {
            super(out);
        }

        // This is the only write() method called by BufferedOutputStream, so we
        // trap calls to it in order to cache the position.
        public void write(byte b[], int off, int len) throws IOException {
            out.write(b, off, len);
            position += len;                            // update position
        }

        public long getPos() throws IOException {
            return position;                            // return cached position
        }

    }

    private static class Buffer extends BufferedOutputStream {
        public Buffer(OutputStream out, int bufferSize) throws IOException {
            super(out, bufferSize);
        }

        public long getPos() throws IOException {
            return ((PositionCache) out).getPos() + this.count;
        }

        // optimized version of write(int)
        public void write(int b) throws IOException {
            if (count >= buf.length) {
                super.write(b);
            } else {
                buf[count++] = (byte) b;
            }
        }

    }

    public FSDataOutputStream(FileSystem fs, File file,
                              boolean overwrite, Configuration conf,
                              int bufferSize)
            throws IOException {
        super(
                new Buffer(new PositionCache(new Summer(fs, file, overwrite, conf)), bufferSize)
        );
    }

    /**
     * Construct without checksums.
     */
    private FSDataOutputStream(FSOutputStream out, Configuration conf) throws IOException {
        this(out, conf.getInt("io.file.buffer.size", 4096));
    }

    /**
     * Construct without checksums.
     */
    private FSDataOutputStream(FSOutputStream out, int bufferSize)
            throws IOException {
        super(new Buffer(new PositionCache(out), bufferSize));
    }

    public long getPos() throws IOException {
        return ((Buffer) out).getPos();
    }

}
