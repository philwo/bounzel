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

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.ActionInputFileCache;
import com.google.devtools.build.lib.actions.Artifact;
import com.google.devtools.build.lib.actions.Artifact.ArtifactExpander;
import com.google.devtools.build.lib.actions.ExecException;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.SpawnResult;
import com.google.devtools.build.lib.actions.SpawnResult.Status;
import com.google.devtools.build.lib.actions.Spawns;
import com.google.devtools.build.lib.actions.cache.Metadata;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.collect.nestedset.NestedSetBuilder;
import com.google.devtools.build.lib.collect.nestedset.NestedSetView;
import com.google.devtools.build.lib.collect.nestedset.Order;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.exec.SpawnInputExpander;
import com.google.devtools.build.lib.exec.SpawnRunner;
import com.google.devtools.build.lib.util.io.FileOutErr;
import com.google.devtools.build.lib.vfs.FileSystem.HashFunction;
import com.google.devtools.build.lib.vfs.FileSystemUtils;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
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
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;

/** A client for the remote execution service. */
@ThreadSafe
class BoundarySpawnRunner implements SpawnRunner {

  private final Path execRoot;
  private final SpawnRunner fallbackRunner;
  private final Channel channel;
  private final ByteStreamUploader uploader;
  private final SpawnInputExpander spawnInputExpander = new SpawnInputExpander(false);
  private final HashFunction hashFunction;

  BoundarySpawnRunner(Path execRoot, Channel channel, SpawnRunner fallbackRunner, Retrier retrier,
      ListeningScheduledExecutorService retryScheduler) {
    this.execRoot = execRoot;
    this.hashFunction = execRoot.getFileSystem().getDigestFunction();
    this.channel = channel;
    this.fallbackRunner = fallbackRunner;
    this.uploader = new ByteStreamUploader(null, channel, null, Long.MAX_VALUE,
        retrier, retryScheduler);
  }

  private NestedSet inputFiles(
      Spawn spawn,
      ArtifactExpander artifactExpander,
      ActionInputFileCache cache,
      Map<Digest, Object> digestInputMap) throws IOException {

    NestedSet.Builder rootBuilder = NestedSet.newBuilder();

    // Add all regular inputs.
    Queue<NestedSetView<? extends ActionInput>> transitiveInputs = new ArrayDeque<>();
    transitiveInputs.add(new NestedSetView<>(getSpawnInputsAsNestedSet(spawn)));
    while (!transitiveInputs.isEmpty()) {
      NestedSetView<? extends ActionInput> inputs = transitiveInputs.remove();
      NestedSet.Builder inputListBuilder = NestedSet.newBuilder();
      Queue<ActionInput> directInputs = new ArrayDeque<>(inputs.directs());

      while (!directInputs.isEmpty()) {
        ActionInput directInput = directInputs.remove();
        if (directInput instanceof Artifact) {
          Artifact directInputArtifact = (Artifact) directInput;
          if (directInputArtifact.isMiddlemanArtifact() || directInputArtifact.isTreeArtifact()) {
            ArrayList<Artifact> expandedArtifacts = new ArrayList<>();
            artifactExpander.expand(directInputArtifact, expandedArtifacts);
            if (!expandedArtifacts.isEmpty()) {
              transitiveInputs.add(new NestedSetView<>(
                  new NestedSetBuilder<Artifact>(Order.STABLE_ORDER).addAll(expandedArtifacts)
                      .build()));
            }
            continue;
          }
        }
        Preconditions.checkArgument(!directInput.getExecPath().isAbsolute(), directInput.getExecPath());
        File file = actionInputToFileProto(directInput, directInput.getExecPath(), cache);
        inputListBuilder.addFiles(file);
        digestInputMap.put(file.getContentDigest(), directInput);
      }

      if (inputListBuilder.getFilesCount() > 0) {
        rootBuilder.addNestedsets(hashAndMemorize(inputListBuilder.build(), digestInputMap));
      }

      transitiveInputs.addAll(inputs.transitives());
    }

    // Add all runfiles to inputs.
    Map<PathFragment, ActionInput> inputMap = new HashMap<>();
    spawnInputExpander.addRunfilesToInputs(inputMap, spawn.getRunfilesSupplier(), cache);

    // Add all inputs from fileset manifests.
    for (Artifact manifest : spawn.getFilesetManifests()) {
      spawnInputExpander.parseFilesetManifest(inputMap, manifest, "");
    }

    for (Entry<PathFragment, ActionInput> entry : inputMap.entrySet()) {
      ActionInput input = entry.getValue();
      File file = actionInputToFileProto(input, entry.getKey(), cache);
      rootBuilder.addFiles(file);
      digestInputMap.put(file.getContentDigest(), input);
    }

    return rootBuilder.build();
  }

  private com.google.devtools.build.lib.collect.nestedset.NestedSet<? extends ActionInput> getSpawnInputsAsNestedSet(Spawn spawn) {
    Iterable<? extends ActionInput> spawnInputs = spawn.getInputFiles();
    if (spawnInputs instanceof com.google.devtools.build.lib.collect.nestedset.NestedSet) {
      return (com.google.devtools.build.lib.collect.nestedset.NestedSet<ActionInput>) spawnInputs;
    } else {
      return new NestedSetBuilder<ActionInput>(Order.STABLE_ORDER).addAll(spawnInputs).build();
    }
  }

  File actionInputToFileProto(ActionInput input, PathFragment targetLocation, ActionInputFileCache cache)
      throws IOException {
    final Digest digest;
    final long size;
    if (input instanceof VirtualActionInput) {
      VirtualActionInput vInput = (VirtualActionInput) input;
      ByteString data = vInput.getBytes();
      digest = Digests.computeDigest(hashFunction, data.toByteArray());
      size = data.size();
    } else if (input == SpawnInputExpander.EMPTY_FILE) {
      digest = Digests.computeDigest(hashFunction, new byte[0]);
      size = 0;
    } else {
      Metadata metadata = cache.getMetadata(input);
      if (metadata == null) {
        throw new IllegalStateException();
      }
      digest = Digests.buildDigest(hashFunction, metadata.getDigest(), metadata.getSize());
      size = metadata.getSize();
    }

    return File.newBuilder()
        .setPath(targetLocation.getPathString())
        .setSize(size)
        .setContentDigest(digest)
        .setIsExecutable(true)
        .build();
  }

  private Command buildCommand(List<String> arguments, ImmutableMap<String, String> env) {
    Command.Builder command = Command.newBuilder();
    command.addAllArguments(arguments);
    // Sort the environment pairs by variable name.
    SortedSet<String> variables = new TreeSet<>(env.keySet());
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

    Digest inputsHash = hashAndMemorize(inputs, digestInputMap);
    Digest commandHash = hashAndMemorize(command, digestInputMap);
    Digest outputsHash = hashAndMemorize(outputs, digestInputMap);

    builder.setNestedsetRoot(inputsHash);
    builder.setCommandDigest(commandHash);
    builder.setOutputFiles(outputsHash);

    return builder.build();
  }

  private Digest hashAndMemorize(Message message, Map<Digest, Object> digestInputMap) {
    byte[] serializedMessage = message.toByteArray();
    Digest messageHash = Digests.computeDigest(hashFunction, serializedMessage);
    digestInputMap.put(messageHash, serializedMessage);
    return messageHash;
  }

  private Chunker chunkerFromDigest(Digest digest, Map<Digest, Object> digestInputMap,
      ActionInputFileCache cache) throws IOException {
    Object input = digestInputMap.get(digest);

    if (input instanceof byte[]) {
      return new Chunker(Digests.digestHashFunctionToHashFunction(digest.getFunction()), (byte[]) input);
    } else if (input instanceof VirtualActionInput) {
      return new Chunker(Digests.digestHashFunctionToHashFunction(digest.getFunction()), ((VirtualActionInput) input).getBytes().toByteArray());
    } else if (input instanceof ActionInput) {
      return new Chunker((ActionInput) input, cache, execRoot);
    } else {
      throw new IllegalStateException(input.getClass().getName());
    }
  }

  static String resourceName(Digest digest) {
    HashCode hash = HashCode.fromBytes(digest.getHash().toByteArray());
    return String.format("blobs/%s/%s/%d", digest.getFunction().name(), hash, digest.getSizeBytes());
  }

  // private void downloadOutputs(ActionResult result, FileOutErr outErr)
  //     throws IOException, ExecException, InterruptedException {
  //   ByteStreamStub byteStreamService = ByteStreamGrpc.newStub(channel);
  //
  //   List<ListenableFuture<Void>> downloads = new ArrayList<>();
  //   for (File output : result.getOutputFilesList()) {
  //     Path path = execRoot.getRelative(output.getPath());
  //     FileSystemUtils.createDirectoryAndParents(path.getParentDirectory());
  //     Digest digest = output.getContentDigest();
  //     downloads.add(download(byteStreamService, digest, path.getOutputStream()));
  //   }
  //
  //   downloads.add(download(byteStreamService, result.getStdoutDigest(), outErr.getOutputStream()));
  //   downloads.add(download(byteStreamService, result.getStderrDigest(), outErr.getErrorStream()));
  //
  //   try {
  //     Futures.allAsList(downloads).get();
  //   } catch (ExecutionException e) {
  //     throw new EnvironmentalExecException("Download of outputs failed", e.getCause());
  //   }
  // }

  // private ListenableFuture<Void> download(ByteStreamStub byteStreamService,
  //     Digest digest, OutputStream out) {
  //   SettableFuture<Void> f = SettableFuture.create();
  //   ReadRequest request = ReadRequest.newBuilder().setResourceName(resourceName(digest)).build();
  //   byteStreamService.read(request, new StreamObserver<ReadResponse>() {
  //     @Override
  //     public void onNext(ReadResponse readResponse) {
  //       try {
  //         readResponse.getData().writeTo(out);
  //       } catch (IOException e) {
  //         onError(e);
  //       }
  //     }
  //
  //     @Override
  //     public void onError(Throwable throwable) {
  //       try {
  //         if (out != null) {
  //           out.close();
  //         }
  //       } catch (IOException e) {
  //         // Intentionally left empty.
  //       } finally {
  //         f.setException(throwable);
  //       }
  //     }
  //
  //     @Override
  //     public void onCompleted() {
  //       try {
  //         if (out != null) {
  //           out.close();
  //         }
  //         f.set(null);
  //       } catch (IOException e) {
  //         f.setException(e);
  //       }
  //     }
  //   });
  //   return f;
  // }

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
        new HashingOutputStream(Digests.hashFunctionFromDigest(stdoutDigest), outErr.getOutputStream());
    outFiles.put(result.getStdoutDigest(), new DownloadState(stdout, null));

    Digest stderrDigest = result.getStderrDigest();
    HashingOutputStream stderr =
        new HashingOutputStream(Digests.hashFunctionFromDigest(stderrDigest), outErr.getErrorStream());
    outFiles.put(result.getStderrDigest(), new DownloadState(stderr, null));

    return outFiles;
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
        outFile.out = new HashingOutputStream(Digests.hashFunctionFromDigest(fileDigest),
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
    if (!Spawns.mayBeExecutedRemotely(spawn)) {
      return fallbackRunner.exec(spawn, policy);
    }
    policy.report(ProgressStatus.EXECUTING, "boundary");

    Map<Digest, Object> digestInputMap = new HashMap<>();

    NestedSet inputs =
        inputFiles(
            spawn,
            policy.getArtifactExpander(),
            policy.getActionInputFileCache(),
            digestInputMap);
    Command command = buildCommand(spawn.getArguments(), spawn.getEnvironment());
    OutputFiles outputs = buildOutputFiles(spawn.getOutputFiles());

    Action action = buildAction(inputs, command, outputs, digestInputMap);

    ActionExecutionBlockingStub executionService = ActionExecutionGrpc.newBlockingStub(channel);

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
            uploads.add(chunkerFromDigest(missing, digestInputMap, policy.getActionInputFileCache()));
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
