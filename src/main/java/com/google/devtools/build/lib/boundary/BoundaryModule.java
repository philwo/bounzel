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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.authandtls.AuthAndTLSOptions;
import com.google.devtools.build.lib.authandtls.GrpcUtils;
import com.google.devtools.build.lib.buildeventstream.PathConverter;
import com.google.devtools.build.lib.buildtool.BuildRequest;
import com.google.devtools.build.lib.events.Event;
import com.google.devtools.build.lib.exec.ExecutorBuilder;
import com.google.devtools.build.lib.runtime.BlazeModule;
import com.google.devtools.build.lib.runtime.Command;
import com.google.devtools.build.lib.runtime.CommandEnvironment;
import com.google.devtools.build.lib.runtime.ServerBuilder;
import com.google.devtools.build.lib.util.AbruptExitException;
import com.google.devtools.build.lib.util.ExitCode;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.common.options.OptionsBase;
import com.google.devtools.common.options.OptionsProvider;
import de.geheimspeicher.boundary.DigestProto.Digest;
import io.grpc.ManagedChannel;
import java.io.IOException;

/** BoundaryModule provides distributed cache and remote execution for Bazel. */
public final class BoundaryModule extends BlazeModule {

  @VisibleForTesting
  static final class CasPathConverter implements PathConverter {
    // Not final; unfortunately, the Bazel startup process requires us to create this object before
    // we have the options available, so we have to create it first, and then set the options
    // afterwards. At the time of this writing, I believe that we aren't using the PathConverter
    // before the options are available, so this should be safe.
    // TODO(ulfjack): Change the Bazel startup process to make the options available when we create
    // the PathConverter.
    BoundaryOptions options;

    @Override
    public String apply(Path path) {
      if (options == null || options.boundaryFrontend == null) {
        return null;
      }
      String server = options.boundaryFrontend;
      try {
        Digest digest = Digests.computeDigest(path);
        return String.format(
            "bytestream://%s/blobs/%s/%d",
            server,
            digest.getHash(),
            digest.getSizeBytes());

      } catch (IOException e) {
        // TODO(ulfjack): Don't fail silently!
        return null;
      }
    }
  }

  private final CasPathConverter converter = new CasPathConverter();

  private BoundaryActionContextProvider actionContextProvider;

  @Override
  public void serverInit(OptionsProvider startupOptions, ServerBuilder builder)
      throws AbruptExitException {
    builder.addPathToUriConverter(converter);
  }

  @Override
  public void beforeCommand(CommandEnvironment env) {
    env.getEventBus().register(this);
    BoundaryOptions boundaryOptions = env.getOptions().getOptions(BoundaryOptions.class);
    AuthAndTLSOptions authAndTlsOptions = env.getOptions().getOptions(AuthAndTLSOptions.class);
    converter.options = boundaryOptions;

    // Quit if no remote options specified.
    if (boundaryOptions == null) {
      return;
    }

    try {
      ManagedChannel ch = GrpcUtils.newChannel(boundaryOptions.boundaryFrontend, authAndTlsOptions);
      actionContextProvider = new BoundaryActionContextProvider(env, ch);
    } catch (IOException e) {
      env.getReporter().handle(Event.error(e.getMessage()));
      env.getBlazeModuleEnvironment().exit(new AbruptExitException(ExitCode.COMMAND_LINE_ERROR));
    }
  }

  @Override
  public void executorInit(CommandEnvironment env, BuildRequest request, ExecutorBuilder builder) {
    if (actionContextProvider != null) {
      builder.addActionContextProvider(actionContextProvider);
    }
  }

  @Override
  public Iterable<Class<? extends OptionsBase>> getCommandOptions(Command command) {
    return "build".equals(command.name())
        ? ImmutableList.<Class<? extends OptionsBase>>of(
            BoundaryOptions.class, AuthAndTLSOptions.class)
        : ImmutableList.<Class<? extends OptionsBase>>of();
  }
}
