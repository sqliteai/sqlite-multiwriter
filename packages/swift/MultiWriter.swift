import Foundation

/// sqlite-multiwriter: SQLite with more writers (threads and processes), as a loadable extension.
///
/// The package carries the extension as a framework. It is loaded into a SQLite that allows extensions
/// (the one you build yourself, or a package that exposes `sqlite3_load_extension`):
///
///     sqlite3_enable_load_extension(db, 1)
///     sqlite3_load_extension(db, MultiWriter.extensionPath!, MultiWriter.entryPoint, nil)
///     // then open the databases with the VFS:  file:app.db?vfs=multiwriter&mw=2
public enum MultiWriter {
    /// The name of the function that SQLite calls when the extension is loaded.
    public static let entryPoint = "sqlite3_multiwriter_init"
    /// The same, and the VFS becomes the default one.
    public static let defaultEntryPoint = "sqlite3_multiwriter_default_init"

    /// The path of the binary of the framework, to pass to `sqlite3_load_extension`; nil if it is not in the app.
    public static var extensionPath: String? {
        for bundle in Bundle.allFrameworks + [Bundle.main] {
            if bundle.bundleIdentifier == "io.sqlitecloud.multiwriter", let p = bundle.executablePath { return p }
        }
        return Bundle(identifier: "io.sqlitecloud.multiwriter")?.executablePath
    }
}
