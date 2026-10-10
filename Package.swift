// swift-tools-version: 5.9
// The Swift package of sqlite-multiwriter: the XCFramework of a release (iOS, iOS simulator, Mac Catalyst, macOS) and a few lines of Swift to find it.
// The release job of the CI puts the url and the checksum of the XCFramework of the version that it released on the next two lines.

import PackageDescription

let package = Package(
    name: "MultiWriter",
    platforms: [.macOS(.v11), .iOS(.v11), .macCatalyst(.v14)],
    products: [
        .library(name: "MultiWriter", targets: ["MultiWriter"])
    ],
    targets: [
        .binaryTarget(
            name: "MultiWriterBinary",
            url: "https://github.com/sqliteai/sqlite-multiwriter/releases/download/0.6.1/multiwriter-apple-xcframework-0.6.1.zip",
            checksum: "c8d105716fc21450fb27cded6e61b2f86c344a4a3dc8f906d604bbd39597f199"
        ),
        .target(
            name: "MultiWriter",
            dependencies: ["MultiWriterBinary"],
            path: "packages/swift"
        ),
    ]
)
