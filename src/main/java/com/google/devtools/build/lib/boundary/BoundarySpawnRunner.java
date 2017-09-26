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
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
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
import de.geheimspeicher.boundary.BoundaryProto.MissingInputs;
import de.geheimspeicher.boundary.BoundaryProto.NestedSet;
import de.geheimspeicher.boundary.DigestProto.Digest;
import de.geheimspeicher.boundary.FileProto.File;
import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
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
      } else {
        Metadata metadata = cache.getMetadata(input);
        if (metadata == null) {
          throw new IllegalStateException();
        }
        digest = Digests.buildDigest(metadata.getDigest(), metadata.getSize());
        size = metadata.getSize();
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

    digestInputMap.put(inputsHash, inputs);
    digestInputMap.put(commandHash, command);
    digestInputMap.put(outputsHash, outputs);

    builder.setNestedsetRoot(inputsHash);
    builder.setCommandDigest(commandHash);
    builder.setOutputFiles(outputsHash);

    return builder.build();
  }

  private Chunker fromDigest(Digest digest, Map<Digest, Object> digestInputMap,
      ActionInputFileCache cache) throws IOException {
    Object input = digestInputMap.get(digest);

    if (input instanceof byte[]) {
      return new Chunker((byte[]) input);
    } else if (input instanceof VirtualActionInput) {
      return new Chunker(((VirtualActionInput) input).getBytes().toByteArray());
    } else if (input instanceof ActionInput) {
      return new Chunker((ActionInput) input, cache, execRoot);
    } else {
      throw new IllegalStateException();
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

    ExecuteRequest executeReq = ExecuteRequest.newBuilder().setActionMessage(action).build();
    Iterator<ExecuteResponse> iter = executionService.execute(executeReq);
    ActionResult result = null;
    try {
      while (iter.hasNext()) {
        ExecuteResponse response = iter.next();
        if (response.hasActionResult()) {
          result = response.getActionResult();
          break;
        } else if (response.hasMissingInputs()) {
          MissingInputs missingInputs = response.getMissingInputs();
          List<Chunker> uploads = new ArrayList<>();
          for (Digest missing : missingInputs.getDigestList()) {
            uploads.add(fromDigest(missing, digestInputMap, policy.getActionInputFileCache()));
          }
          uploader.uploadBlobs(uploads);
        }
      }
    } catch (StatusRuntimeException e) {
      // Failed.
    }

    Preconditions.checkState(result != null);

    downloadOutputs(result, policy.getFileOutErr());

    return new SpawnResult.Builder()
        .setStatus(Status.SUCCESS)  // Even if the action failed with non-zero exit code.
        .setExitCode(result.getExitCode())
        .build();
  }
}
