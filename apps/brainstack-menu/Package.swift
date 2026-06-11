// swift-tools-version: 5.9
import PackageDescription

let package = Package(
  name: "BrainstackMenu",
  platforms: [
    .macOS(.v13)
  ],
  targets: [
    .target(
      name: "BrainstackMenuCore",
      path: "Sources/BrainstackMenuCore"
    ),
    .executableTarget(
      name: "BrainstackMenu",
      dependencies: ["BrainstackMenuCore"],
      path: "Sources/BrainstackMenu"
    ),
    .testTarget(
      name: "BrainstackMenuCoreTests",
      dependencies: ["BrainstackMenuCore"],
      path: "Tests/BrainstackMenuCoreTests",
      resources: [
        .copy("Fixtures")
      ]
    )
  ]
)
