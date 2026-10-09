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
            url: "https://github.com/sqliteai/sqlite-multiwriter/releases/download/0.5.2/multiwriter-apple-xcframework-0.5.2.zip",
            checksum: "c9766386289347768e76c1c3ecc1ab3250f971548cf73d89a2ef29a76355955b"
        ),
        .target(
            name: "MultiWriter",
            dependencies: ["MultiWriterBinary"],
            path: "packages/swift"
        ),
    ]
)
