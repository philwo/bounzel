// Copyright 2017 The Bazel Authors. All rights reserved.
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

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamStub;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputFileCache;
import com.google.devtools.build.lib.actions.EnvironmentalExecException;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.SpawnResult;
import com.google.devtools.build.lib.actions.SpawnResult.Status;
import com.google.devtools.build.lib.actions.cache.Metadata;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.exec.SpawnInputExpander;
import com.google.devtools.build.lib.exec.SpawnRunner;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.protobuf.ByteString;
import de.geheimspeicher.boundary.ActionExecutionGrpc;
import de.geheimspeicher.boundary.ActionExecutionGrpc.ActionExecutionBlockingStub;
import de.geheimspeicher.boundary.BoundaryProto.Action;
import de.geheimspeicher.boundary.BoundaryProto.Action.OutputFiles;
import de.geheimspeicher.boundary.BoundaryProto.ActionResult;
import de.geheimspeicher.boundary.BoundaryProto.Command;
import de.geheimspeicher.boundary.BoundaryProto.ExecuteRequest;
import de.geheimspeicher.boundary.BoundaryProto.ExecuteResponse;
import de.geheimspeicher.boundary.BoundaryProto.ExecuteResponse.ResultCase;
import de.geheimspeicher.boundary.BoundaryProto.MissingInputs;
import de.geheimspeicher.boundary.BoundaryProto.NestedSet;
import de.geheimspeicher.boundary.CassyProto.CassyReadResponse;
import de.geheimspeicher.boundary.DigestProto.Digest;
import de.geheimspeicher.boundary.FileProto.File;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

/** A client for the remote execution service. */
@ThreadSafe
class BoundarySpawnRunner implements SpawnRunner {

  private final Path execRoot;
  private final SpawnRunner fallbackRunner;
  private final Channel channel;
  private final Retrier retrier;
  private final ListeningScheduledExecutorService retryScheduler;

  BoundarySpawnRunner(Path execRoot, Channel channel, SpawnRunner fallbackRunner, Retrier retrier,
      ListeningScheduledExecutorService retryScheduler) {
    this.execRoot = execRoot;
    this.channel = channel;
    this.fallbackRunner = fallbackRunner;
    this.retrier = retrier;
    this.retryScheduler = retryScheduler;
  }

  private NestedSet inputFiles(Map<PathFragment, ActionInput> inputs,
      ActionInputFileCache cache, Map<Digest, Object> digestInputMap) throws IOException {
    NestedSet.Builder builder = NestedSet.newBuilder();
    for (Entry<PathFragment, ActionInput> entry : inputs.entrySet()) {
      final Digest digest;
      final long size;
      ActionInput input = entry.getValue();
      if (input instanceof VirtualActionInput) {
        VirtualActionInput vInput = (VirtualActionInput) input;
        ByteString data = vInput.getBytes();
        digest = Digests.computeDigest(data.toByteArray());
        size = data.size();
      } else if (input != SpawnInputExpander.EMPTY_FILE) {
        Metadata metadata = cache.getMetadata(input);
        if (metadata == null) {
          throw new IllegalStateException();
        }
        digest = Digests.buildDigest(metadata.getDigest(), metadata.getSize());
        size = metadata.getSize();
      } else {
        digest = Digests.computeDigest(new byte[0]);
        size = 0;
      }

      String path = entry.getKey().getPathString();
      File file = File.newBuilder()
          .setPath(path)
          .setContentDigest(digest)
          .setIsExecutable(true)
          .setSize(size)
          .build();
      builder.addFiles(file);

      digestInputMap.put(digest, input);
    }
    return builder.build();
  }

  private Command buildCommand(List<String> arguments, ImmutableMap<String, String> env) {
    Command.Builder command = Command.newBuilder();
    command.addAllArguments(arguments);
    // Sorting the environment pairs by variable name.
    TreeSet<String> variables = new TreeSet<>(env.keySet());
    for (String var : variables) {
      command.addEnvironmentVariablesBuilder().setName(var).setValue(env.get(var));
    }
    return command.build();
  }

  private OutputFiles buildOutputFiles(Collection<? extends ActionInput> outputs) {
    OutputFiles.Builder builder = OutputFiles.newBuilder();
    for (ActionInput output : outputs) {
      builder.addPaths(output.getExecPathString());
    }
    return builder.build();
  }

  private Action buildAction(NestedSet inputs, Command command, OutputFiles outputs,
      Map<Digest, Object> digestInputMap) {
    Action.Builder builder = Action.newBuilder();

    Digest inputsHash = Digests.computeDigest(inputs.toByteArray());
    Digest commandHash = Digests.computeDigest(command.toByteArray());
    Digest outputsHash = Digests.computeDigest(outputs.toByteArray());

    digestInputMap.put(inputsHash, inputs.toByteArray());
    digestInputMap.put(commandHash, command.toByteArray());
    digestInputMap.put(outputsHash, outputs.toByteArray());

    builder.setNestedsetRoot(inputsHash);
    builder.setCommandDigest(commandHash);
    builder.setOutputFiles(outputsHash);

    return builder.build();
  }

  private Chunker hashFuncFromDigest(Digest digest, Map<Digest, Object> digestInputMap,
      ActionInputFileCache cache) throws IOException {
    Object input = digestInputMap.get(digest);

    if (input instanceof byte[]) {
      return new Chunker((byte[]) input);
    } else if (input instanceof VirtualActionInput) {
      return new Chunker(((VirtualActionInput) input).getBytes().toByteArray());
    } else if (input instanceof ActionInput) {
      return new Chunker((ActionInput) input, cache, execRoot);
    } else {
      throw new IllegalStateException(input.getClass().getName());
    }
  }

  private String resourceName(Digest digest) {
    String hash = HashCode.fromBytes(digest.getHash().toByteArray()).toString();
    return "blobs/" + hash + "/" + digest.getSizeBytes();
  }

  private void downloadOutputs(ActionResult result, FileOutErr outErr)
      throws IOException, ExecException, InterruptedException {
    ByteStreamStub byteStreamService = ByteStreamGrpc.newStub(channel);

    List<ListenableFuture<Void>> downloads = new ArrayList<>();
    for (File output : result.getOutputFilesList()) {
      Path path = execRoot.getRelative(output.getPath());
      FileSystemUtils.createDirectoryAndParents(path.getParentDirectory());
      Digest digest = output.getContentDigest();
      downloads.add(download(byteStreamService, digest, path.getOutputStream()));
    }

    downloads.add(download(byteStreamService, result.getStdoutDigest(), outErr.getOutputStream()));
    downloads.add(download(byteStreamService, result.getStderrDigest(), outErr.getErrorStream()));

    try {
      Futures.allAsList(downloads).get();
    } catch (ExecutionException e) {
      throw new EnvironmentalExecException("Download of outputs failed", e.getCause());
    }
  }

  private ListenableFuture<Void> download(ByteStreamStub byteStreamService,
      Digest digest, OutputStream out) {
    SettableFuture<Void> f = SettableFuture.create();
    ReadRequest request = ReadRequest.newBuilder().setResourceName(resourceName(digest)).build();
    byteStreamService.read(request, new StreamObserver<ReadResponse>() {
      @Override
      public void onNext(ReadResponse readResponse) {
        try {
          readResponse.getData().writeTo(out);
        } catch (IOException e) {
          onError(e);
        }
      }

      @Override
      public void onError(Throwable throwable) {
        if (out != null) {
          try {
            out.close();
          } catch (IOException e) {
            // Intentionally left empty.
          } finally {
            f.setException(throwable);
          }
        }
      }

      @Override
      public void onCompleted() {
        try {
          out.close();
          f.set(null);
        } catch (IOException e) {
          f.setException(e);
        }
      }
    });
    return f;
  }

  enum ResponseState {
    MISSING_INPUTS,
    ACTION_RESULT,
    OUTPUTS
  }

  static class DownloadState {

    static final HashingOutputStream NOT_INITIALIZED =
        new HashingOutputStream(Hashing.sha256(), ByteStreams.nullOutputStream());

    // The OutputStream to write downloaded contents to.
    // Using a hashing stream so that we can easily verify the downloaded result is correct.
    private HashingOutputStream out;
    // The offset of the last write.
    private long offset;
    // The file being downloaded.
    private final File file;

    DownloadState(HashingOutputStream out, File file) {
      this.out = out;
      this.file = file;
    }
  }

  private ResponseState transition(ResultCase input, ResponseState current) {
    if (current == null) {
      if (input.equals(ResultCase.MISSING_INPUTS)) {
        return ResponseState.MISSING_INPUTS;
      }
      if (input.equals(ResultCase.ACTION_RESULT)) {
        return ResponseState.ACTION_RESULT;
      }
    } else if (current.equals(ResponseState.MISSING_INPUTS)) {
      if (input.equals(ResultCase.MISSING_INPUTS)) {
        return ResponseState.MISSING_INPUTS;
      }
      if (input.equals(ResultCase.ACTION_RESULT)) {
        return ResponseState.ACTION_RESULT;
      }
    } else if (current.equals(ResponseState.ACTION_RESULT)) {
      if (input.equals(ResultCase.OUTPUT)) {
        return ResponseState.OUTPUTS;
      }
    } else if (current.equals(ResponseState.OUTPUTS)) {
      if (input.equals(ResultCase.OUTPUT)) {
        return ResponseState.OUTPUTS;
      }
    }
    throw new IllegalStateException("Received illegal sequence of messages from server. In state "
        + current.name() + " received " + input.name());
  }

  private Multimap<Digest, DownloadState> initializeDownloadInfos(ActionResult result,
      FileOutErr outErr) {
    Multimap<Digest, DownloadState> outFiles = ArrayListMultimap.create();
    for (File f : result.getOutputFilesList()) {
      outFiles.put(f.getContentDigest(), new DownloadState(DownloadState.NOT_INITIALIZED, f));
    }

    Digest stdoutDigest = result.getStdoutDigest();
    HashingOutputStream stdout =
        new HashingOutputStream(hashFuncFromDigest(stdoutDigest), outErr.getOutputStream());
    outFiles.put(result.getStdoutDigest(), new DownloadState(stdout, null));
    Digest stderrDigest = result.getStderrDigest();
    HashingOutputStream stderr =
        new HashingOutputStream(hashFuncFromDigest(stderrDigest), outErr.getErrorStream());
    outFiles.put(result.getStderrDigest(), new DownloadState(stderr, null));
    return outFiles;
  }

  private HashFunction hashFuncFromDigest(Digest digest) {
    if (digest.getFunction().equals(Digest.HashFunction.MD5)) {
      return Hashing.md5();
    } else if (digest.getFunction().equals(Digest.HashFunction.SHA256)) {
      return Hashing.sha256();
    } else {
      throw new IllegalArgumentException("Unsupported hash function: " + digest.getFunction().name());
    }
  }

  private DownloadState getDownloadState(Multimap<Digest, DownloadState> outFilesMap,
      Digest fileDigest, long offset) throws IOException {
    Collection<DownloadState> outFiles = outFilesMap.get(fileDigest);
    if (outFiles.isEmpty()) {
      throw new IOException("Unknown output digest: " + fileDigest);
    }
    for (DownloadState outFile : outFiles) {
      if (outFile.out == DownloadState.NOT_INITIALIZED) {
        Path path = execRoot.getRelative(outFile.file.getPath());
        if (!path.getParentDirectory().exists()) {
          FileSystemUtils.createDirectoryAndParents(path.getParentDirectory());
        }
        outFile.out = new HashingOutputStream(hashFuncFromDigest(fileDigest),
            path.getOutputStream());
      }
      if (outFile.offset == offset) {
        return outFile;
      }
    }
    throw new IOException("Unknown output digest: " + fileDigest);
  }

  @Override
  public SpawnResult exec(Spawn spawn, SpawnExecutionPolicy policy)
      throws ExecException, InterruptedException, IOException {
    if (!spawn.isRemotable()) {
      return fallbackRunner.exec(spawn, policy);
    }
    policy.report(ProgressStatus.EXECUTING, "boundary");

    Map<Digest, Object> digestInputMap = new HashMap<>();

    NestedSet inputs =
        inputFiles(policy.getInputMapping(), policy.getActionInputFileCache(), digestInputMap);
    Command command = buildCommand(spawn.getArguments(), spawn.getEnvironment());
    OutputFiles outputs = buildOutputFiles(spawn.getOutputFiles());

    Action action = buildAction(inputs, command, outputs, digestInputMap);

    ActionExecutionBlockingStub executionService = ActionExecutionGrpc.newBlockingStub(channel);
    ByteStreamUploader uploader = new ByteStreamUploader(null, channel, null, Long.MAX_VALUE,
        retrier, retryScheduler);

    ExecuteRequest executeReq = ExecuteRequest.newBuilder().setActionMessage(action).setPushOutputs(true).build();
    Iterator<ExecuteResponse> iter = executionService.execute(executeReq);
    ActionResult result = null;

    ResponseState state = null;
    Multimap<Digest, DownloadState> outFiles = null;
    while (iter.hasNext()) {
      ExecuteResponse response = iter.next();
      state = transition(response.getResultCase(), state);
      switch (state) {
        case MISSING_INPUTS:
          MissingInputs missingInputs = response.getMissingInputs();
          List<Chunker> uploads = new ArrayList<>();
          for (Digest missing : missingInputs.getDigestList()) {
            uploads.add(hashFuncFromDigest(missing, digestInputMap, policy.getActionInputFileCache()));
          }
          uploader.uploadBlobs(uploads);
          break;

        case ACTION_RESULT:
          result = response.getActionResult();
          outFiles = initializeDownloadInfos(result, policy.getFileOutErr());
          break;

        case OUTPUTS:
          Preconditions.checkState(result != null);
          Preconditions.checkState(outFiles != null);

          CassyReadResponse read = response.getOutput();
          Digest fileDigest = read.getDigest();
          DownloadState download = getDownloadState(outFiles, fileDigest, read.getOffset());
          read.getData().writeTo(download.out);
          download.offset += read.getData().size();
          if (read.getEof()) {
            download.out.close();
            HashCode expectedHash = HashCode.fromBytes(fileDigest.getHash().toByteArray());
            HashCode actualHash = download.out.hash();
            if (!expectedHash.equals(actualHash)) {
              throw new IOException("Hash codes don't match.");
            }
            outFiles.remove(fileDigest, download);
          }
          break;
      }
    }

    if (outFiles == null || !outFiles.isEmpty()) {
      // Not all files were pushed fully.
      // TODO: Download missing outputs from Cassy.
      throw new IOException("Not all output files were pushed.");
    }

    return new SpawnResult.Builder()
        .setStatus(Status.SUCCESS)  // Even if the action failed with non-zero exit code.
        .setExitCode(result.getExitCode())
        .build();
  }
}
