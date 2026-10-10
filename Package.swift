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
            url: "https://github.com/sqliteai/sqlite-multiwriter/releases/download/0.6.2/multiwriter-apple-xcframework-0.6.2.zip",
            checksum: "2dc045d136dba3f0cf902d69066111865d9c735df03eb9893ea9535ef52406c1"
        ),
        .target(
            name: "MultiWriter",
            dependencies: ["MultiWriterBinary"],
            path: "packages/swift"
        ),
    ]
)
