// swift-tools-version:4.0

import PackageDescription

let package = Package(
    name: "RedShot",
    products: [
        .library(name: "RedShot", targets: ["RedShot"])
    ],
    dependencies: [
        .package(url: "https://github.com/OperatorFoundation/Datable.git", from: "0.0.8")
    ],
    targets:[
        .target(name:"RedShot", dependencies: ["Datable"]),
        .testTarget(name: "RedShotTests", dependencies: ["RedShot"])
    ]
)
