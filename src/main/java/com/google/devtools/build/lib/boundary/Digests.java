// Copyright 2016 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.lib.boundary;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.MetadataProvider;
import com.google.devtools.build.lib.actions.cache.DigestUtils;
import com.google.devtools.build.lib.actions.cache.Metadata;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.vfs.FileSystem;
import com.google.devtools.build.lib.vfs.FileSystem.HashFunction;
import com.google.devtools.build.lib.vfs.Path;
import com.google.protobuf.ByteString;
import de.geheimspeicher.boundary.DigestProto.Digest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/** Helper methods relating to computing Digest messages for remote execution. */
@ThreadSafe
final class Digests {
  private Digests() {}

  private static Digest.HashFunction hashFunctionToDigestHashFunction(HashFunction hashFunction) {
    switch (hashFunction) {
      case MD5:
        return Digest.HashFunction.MD5;
      case SHA256:
        return Digest.HashFunction.SHA256;
      default:
        throw new IllegalStateException("not implemented yet");
    }
  }

  static com.google.common.hash.HashFunction hashFunctionFromDigest(Digest digest) {
    switch (digest.getFunction()) {
      case SHA256:
        return Hashing.sha256();
      case MD5:
        return Hashing.md5();
      default:
        throw new IllegalArgumentException("Unsupported hash function: " + digest.getFunction().name());
    }
  }

  public static Digest computeDigest(byte[] blob) {
    HashFunction hashFunction = FileSystem.getDigestFunction();
    byte[] hash = hashFunction.getHash().hashBytes(blob).asBytes();
    return buildDigest(hashFunction, hash, blob.length);
  }

  public static Digest computeDigest(Path file) throws IOException {
    long fileSize = file.getFileSize();
    byte[] hash = DigestUtils.getDigestOrFail(file, fileSize);
    return buildDigest(FileSystem.getDigestFunction(), hash, fileSize);
  }

  public static Digest getDigestFromInputCache(ActionInput input, MetadataProvider cache)
      throws IOException {
    Metadata metadata = cache.getMetadata(input);
    return buildDigest(FileSystem.getDigestFunction(), metadata.getDigest(), metadata.getSize());
  }

  public static Digest buildDigest(HashFunction hashFunction, byte[] hash, long sizeBytes) {
    return Digest.newBuilder()
        .setFunction(hashFunctionToDigestHashFunction(hashFunction))
        .setHash(ByteString.copyFrom(hash))
        .setSizeBytes(sizeBytes)
        .build();
  }
}
