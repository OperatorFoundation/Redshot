// swift-tools-version:5.1

import PackageDescription

let package = Package(
    name: "RedShot",
    products: [
        .library(name: "RedShot", targets: ["RedShot"])
    ],
    dependencies: [
        .package(url: "https://github.com/OperatorFoundation/Datable.git", from: "3.0.2")
    ],
    targets:[
        .target(name:"RedShot", dependencies: ["Datable"]),
        .testTarget(name: "RedShotTests", dependencies: ["RedShot"])
    ]
)
