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

public enum RedisError: Error {
    case connection(String)
    case response(String)
    case parseResponse
    case typeUnknown
    case emptyResponse
    case noAuthorized
    case failedRead
}

/// Redis Client
public class Redis {
    private var redisSocket: RedisSocket
    private var mutex: DispatchSemaphore
    public static let cr: UInt8 = 0x0D
    public static let lf: UInt8 = 0x0A
    private let hostname: String
    private let port: Int
    private var password: String?
    private var subscriber = [String: RedisSocket]()

    /// Test whether or not the client is connected
    public var isConnected: Bool {
        return self.redisSocket.isConnected
    }

    /// Initializes a `Redis` instance and connects to a Redis server.
    ///
    /// - Parameters:
    ///   - hostname: the server hostname or IP address.
    ///   - port: the port number.
    ///   - password: The password is optional. If `password` is an empty `String` is ignored.
    /// - Throws: if the client can't connect
    public required init(hostname: String, port: Int, password: String? = nil) throws {
		self.hostname = hostname
        self.port = port
        self.password = password

        self.redisSocket = try RedisSocket(hostname: hostname, port: port)
        self.mutex = DispatchSemaphore(value: 1)

        if let password = password, !password.isEmpty {
        	let _:RedisType = try auth(password: password)
        }
    }
    
    @discardableResult public func sendCommand(_ cmd: RedisType, values: [RedisType]) throws -> RedisType {
        self.mutex.wait()
        
        do {
            if !self.isConnected {
                redisSocket = try RedisSocket(hostname: self.hostname, port: self.port)
                if let password = password {
                    let _: Bool = try self.auth(password: password)
                }
            }
            
            try redisSocket.send(star)
            try redisSocket.send("\(values.count + 1)".data)
            try redisSocket.send(rn)
            try sendRedisBulkString(cmd)
            
            for value in values {
                try sendRedisBulkString(value)
            }
            
            let maybeData = redisSocket.read()
            guard let data = maybeData else {
                mutex.signal()
                throw RedisError.failedRead
            }
            
            let copyData = Data(data)
            let bytes = copyData.withUnsafeBytes {
                [UInt8](UnsafeBufferPointer(start: $0, count: data.count))
            }
            
            let parser = Parser(bytes: bytes)
            let result = try parser.parse()
            
            self.mutex.signal()
            
            return result
        } catch {
            mutex.signal()
            throw error
        }
    }
    
    private func sendRedisBulkString(_ value: RedisType) throws {
        do {
            try redisSocket.send(dollar)
            
            let data: Data
            
            switch value
            {
                case let dataValue as Data:
                    data = dataValue
                case let stringValue as String:
                    data = stringValue.data
                case let intValue as Int:
                    let stringValue = String(intValue)
                    data = stringValue.data
                case let floatValue as Float:
                    let stringValue = String(floatValue)
                    data = stringValue.data
                case let doubleValue as Double:
                    let stringValue = String(doubleValue)
                    data = stringValue.data
                default:
                    throw RedisError.failedRead
            }
            
            let strLength = data.count
            try redisSocket.send("\(strLength)".data)
            try redisSocket.send(rn)
            try redisSocket.send(data)
            try redisSocket.send(rn)
        } catch {
            throw error
        }
    }
    
    /// Subscribes the client to the specified channel.
    ///
    /// - Parameters:
    ///   - channel: The channel
    ///   - callback:
    /// - Throws: Errors
    public func subscribe(channel: String, callback:@escaping (RedisType?, Error?) -> Void) throws {
        let subscribeSocket = try RedisSocket(hostname: hostname, port: port)

        if let password = self.password {
            do {
                try subscribeSocket.send("AUTH \(password)\r\n".data)
            } catch {
                throw error
            }
            
            let maybeData = subscribeSocket.read()
            guard let data = maybeData else {
                throw RedisError.noAuthorized
            }
            
            let bytes = data.withUnsafeBytes {
                [UInt8](UnsafeBufferPointer(start: $0, count: data.count))
            }

            let parser = Parser(bytes: bytes)
            let authResponse = try parser.parse()
            guard (authResponse as? String) == "OK" else {
                throw RedisError.noAuthorized
            }
        }

        subscriber[channel] = subscribeSocket

        DispatchQueue.global(qos: .userInitiated).async {
            do {
                try subscribeSocket.send("SUBSCRIBE \(channel)\r\n".data)
                
                while subscribeSocket.isConnected {
                    let maybeData = subscribeSocket.read()
                    guard let data = maybeData else {
                        throw RedisError.noAuthorized
                    }
                    
                    let bytes = data.withUnsafeBytes {
                        [UInt8](UnsafeBufferPointer(start: $0, count: data.count))
                    }
                    if !bytes.isEmpty {
                        do {
                            let parser = Parser(bytes: bytes)
                            callback(try parser.parse(), nil)
                        } catch {
                            callback(nil, error)
                        }
                    }
                }
            } catch {
                callback(nil, error)
            }
        }
    }

    public func unsubscribe(channel: String) {
        if let socket = subscriber[channel] {
            socket.close()
            subscriber.removeValue(forKey: channel)
        }
    }

    /// Disconnect the client as quickly and silently as possible.
    public func close() {
        self.redisSocket.close()
    }

    deinit {
        self.close()
    }
}
