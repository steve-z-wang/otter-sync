import ExpoModulesCore
import Foundation

private class AxtonNativeException: Exception {
  private let message: String

  init(_ message: String) {
    self.message = message
    super.init()
  }

  override var reason: String {
    message
  }
}

/// The wake sink handed to every runtime this module opens. It runs on a Rust
/// runtime thread, so it only schedules the `axtonWake` event on the main
/// queue; the JS Bridge drains in response. `context` is the module,
/// unretained: `OnDestroy` detaches every runtime first, and a detach returns
/// only once no wake is running and none can follow.
private let axtonWake: @convention(c) (UInt64, UnsafeMutableRawPointer?) -> Void = {
  runtime, context in
  guard let context else { return }
  Unmanaged<AxtonNativeModule>.fromOpaque(context).takeUnretainedValue().wake(runtime)
}

public final class AxtonNativeModule: Module {
  /// Runtimes opened and not yet detached, so `OnDestroy` can detach them.
  private let runtimesLock = NSLock()
  private var runtimes = Set<UInt64>()

  public func definition() -> ModuleDefinition {
    Name("AxtonNative")

    Events("axtonWake")

    // The Rust-owned client runtime (#134). All four are synchronous: they
    // admit, drain or detach and never wait on SQLite, which runs on the
    // runtime's own thread.
    Function("runtimeOpen") { (request: String) throws -> String in
      var error: UnsafeMutablePointer<CChar>?
      let context = Unmanaged.passUnretained(self).toOpaque()
      let runtime = request.withCString {
        axton_mobile_runtime_open($0, axtonWake, context, &error)
      }
      let message = self.take(error)
      guard runtime != 0 else {
        throw AxtonNativeException(message ?? "AXTON runtime open failed")
      }
      _ = self.withRuntimes { $0.insert(runtime) }
      return String(runtime)
    }

    Function("runtimeSubmit") { (runtimeId: String, message: String) throws in
      let runtime = try self.runtime(runtimeId)
      var error: UnsafeMutablePointer<CChar>?
      let status = message.withCString {
        axton_mobile_runtime_submit(runtime, $0, &error)
      }
      let failure = self.take(error)
      if status != 0 {
        throw AxtonNativeException(failure ?? "client_closed")
      }
    }

    Function("runtimeDrain") { (runtimeId: String) throws -> String in
      let runtime = try self.runtime(runtimeId)
      return self.take(axton_mobile_runtime_drain(runtime)) ?? "[]"
    }

    Function("runtimeDetach") { (runtimeId: String) throws in
      let runtime = try self.runtime(runtimeId)
      axton_mobile_runtime_detach(runtime)
      self.withRuntimes { _ = $0.remove(runtime) }
    }

    OnDestroy {
      let open = self.withRuntimes { runtimes -> Set<UInt64> in
        let open = runtimes
        runtimes.removeAll()
        return open
      }
      for runtime in open {
        axton_mobile_runtime_detach(runtime)
      }
    }

    AsyncFunction("databasePath") { (name: String) throws -> String in
      do {
        return try MobileDatabasePath.resolve(name: name).path
      } catch MobileDatabasePath.Error.invalidBasename {
        throw AxtonNativeException("database name must be a basename")
      } catch {
        throw AxtonNativeException("could not create Application Support directory: \(error)")
      }
    }
  }

  /// Called from a runtime thread: post the wake on the main queue, never
  /// into Expo from the Rust thread.
  fileprivate func wake(_ runtime: UInt64) {
    DispatchQueue.main.async { [weak self] in
      self?.sendEvent("axtonWake", ["runtimeId": String(runtime)])
    }
  }

  private func runtime(_ runtimeId: String) throws -> UInt64 {
    guard let runtime = UInt64(runtimeId), runtime != 0 else {
      throw AxtonNativeException("client_closed")
    }
    return runtime
  }

  private func withRuntimes<T>(_ body: (inout Set<UInt64>) -> T) -> T {
    runtimesLock.lock()
    defer { runtimesLock.unlock() }
    return body(&runtimes)
  }

  /// Copy and free a string the C ABI returned; each is freed exactly once.
  private func take(_ output: UnsafeMutablePointer<CChar>?) -> String? {
    guard let output else { return nil }
    defer { axton_mobile_free(output) }
    return String(cString: output)
  }
}
