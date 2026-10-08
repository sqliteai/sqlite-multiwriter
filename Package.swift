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
            url: "https://github.com/sqliteai/sqlite-multiwriter/releases/download/0.5.0/multiwriter-apple-xcframework-0.5.0.zip",
            checksum: "e337363072d1fa6d26861045c689d7b3700e6ea2d0cfcb35e36ae1c86185c548"
        ),
        .target(
            name: "MultiWriter",
            dependencies: ["MultiWriterBinary"],
            path: "packages/swift"
        ),
    ]
)
