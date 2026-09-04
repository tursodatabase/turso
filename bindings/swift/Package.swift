// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "Turso",
    platforms: [.iOS(.v16), .macOS(.v13)],
    products: [
        .library(name: "Turso", targets: ["Turso"])
    ],
    targets: [
        .binaryTarget(name: "TursoSdkKit", path: "TursoSdkKit.xcframework"),
        .target(
            name: "Turso",
            dependencies: ["TursoSdkKit"],
            linkerSettings: [
                .linkedFramework("Security"),
                .linkedFramework("CoreServices"),
            ]
        ),
        .testTarget(name: "TursoTests", dependencies: ["Turso"]),
    ]
)
