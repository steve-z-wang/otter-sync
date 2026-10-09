import Foundation

enum MobileDatabasePath {
  enum Error: Swift.Error {
    case invalidBasename
    case applicationSupportUnavailable
  }

  static func resolve(
    name: String,
    fileManager: FileManager = .default,
    applicationSupport: URL? = nil
  ) throws -> URL {
    guard !name.isEmpty,
          name != ".",
          name != "..",
          !name.contains("/"),
          !name.contains("\\"),
          URL(fileURLWithPath: name).lastPathComponent == name
    else {
      throw Error.invalidBasename
    }

    let directory: URL
    if let applicationSupport {
      directory = applicationSupport
    } else {
      guard let resolved = fileManager.urls(for: .applicationSupportDirectory, in: .userDomainMask).first else {
        throw Error.applicationSupportUnavailable
      }
      directory = resolved
    }
    try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
    return directory.appendingPathComponent(name, isDirectory: false)
  }
}
