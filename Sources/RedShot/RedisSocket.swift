//
// This source file is part of the RedShot open source project
//
// Copyright (c) 2017  Bermuda Digital Studio
// Licensed under MIT
//
// See https://github.com/bermudadigitalstudio/Redshot/blob/master/LICENSE for license information
//
//  Created by Laurent Gaches on 12/06/2017.
//

import Foundation
import Dispatch

#if os(Linux)
import FoundationNetworking
#endif

import Datable

class RedisSocket: NSObject {
    let session = URLSession(configuration: .default)
    var stream: URLSessionStreamTask

    var writeGroup: DispatchGroup
    var readGroup: DispatchGroup
    var readBuffer = Data()

    init(hostname: String, port: Int) throws {
        stream = session.streamTask(withHostName: hostname, port: port)
        stream.resume()
        
        writeGroup=DispatchGroup.init()
        readGroup=DispatchGroup.init()
    }

    func read() -> Data? {
        // Refresh the buffer
        readBuffer = Data()
        readGroup.enter()
        readAll()
        readGroup.wait()

        return readBuffer
    }
    
    func readAll(timeout: TimeInterval = 0)
    {
        let maxLength = 4096
        
        stream.readData(ofMinLength: 1, maxLength: maxLength, timeout: timeout) {
            (maybeData, atEof, maybeError) in
            
            guard maybeError == nil
            else {
                self.readGroup.leave()
                return
            }
            
            guard let data = maybeData
            else {
                self.readGroup.leave()
                return
            }
            
            self.readBuffer.append(data)
            
            if data.count == maxLength
            {
                self.readAll(timeout: 2)
            }
            else
            {
                self.readGroup.leave()
            }
        }
    }

    func send(_ datable: Data) throws {
        let data = datable.data
        guard !data.isEmpty else { return }
        
        writeGroup.enter()
        stream.write(data, timeout: 0, completionHandler: {
            (maybeError) in
            
            if maybeError != nil {
                self.close()
            }
            
            self.writeGroup.leave()
        })
        writeGroup.wait()
    }

    func close() {
        stream.cancel()
    }

    var isConnected: Bool {
        return stream.state == .running
    }

    deinit {
        close()
    }
}

extension RedisSocket: URLSessionStreamDelegate {
    func urlSession(_ session: URLSession,
                    readClosedFor streamTask: URLSessionStreamTask) {
        
    }
    
    func urlSession(_ session: URLSession,
                    writeClosedFor streamTask: URLSessionStreamTask) {
        
    }
    
    func urlSession(_ session: URLSession,
                    betterRouteDiscoveredFor streamTask: URLSessionStreamTask) {
        
    }
    
    func urlSession(_ session: URLSession,
                    streamTask: URLSessionStreamTask,
                    didBecome inputStream: InputStream,
                    outputStream: OutputStream) {
        
    }
}
