import Foundation

@main
enum DatabasePathTests {
  static func main() throws {
    let root = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
      .appendingPathComponent(UUID().uuidString, isDirectory: true)
    defer { try? FileManager.default.removeItem(at: root) }

    let first = try MobileDatabasePath.resolve(name: "demo.sqlite", applicationSupport: root)
    let second = try MobileDatabasePath.resolve(name: "demo.sqlite", applicationSupport: root)
    precondition(first == second)
    precondition(first.lastPathComponent == "demo.sqlite")
    precondition(FileManager.default.fileExists(atPath: root.path))

    for invalid in ["", ".", "..", "nested/demo.sqlite", "nested\\demo.sqlite", "/tmp/demo.sqlite"] {
      do {
        _ = try MobileDatabasePath.resolve(name: invalid, applicationSupport: root)
        fatalError("accepted invalid database name: \(invalid)")
      } catch MobileDatabasePath.Error.invalidBasename {
        // Expected.
      }
    }
  }
}
