//
// This source file is part of the RedShot open source project
//
// Copyright (c) 2017  Bermuda Digital Studio
// Licensed under MIT
//
// See https://github.com/bermudadigitalstudio/Redshot/blob/master/LICENSE for license information
//
//  Created by Laurent Gaches on 13/06/2017.
//

import XCTest
import RedShot
import Datable

final class RedShotTests: XCTestCase
{
    #if os(Linux)
    let hostname = "redis"
    let port = 6380
    #else
    let hostname = "localhost"
    let port = 6380
    #endif

    static var allTests = [
        ("testComplexString", testComplexString),
        ("testCommand", testCommand),
        ("testInitWithPassword", testInitWithPassword),
        ("testPush", testPush),
        ("testSubscribe", testSubscribe),
        ("testUnsubscribe", testUnsubscribe),
        ("testIncrement", testIncrement),
        ("testSelect", testSelect),
        ("testhset", testhset)
    ]
    
    func testPing()
    {
        guard let redis = try? Redis(hostname: hostname, port: port, password: nil)
        else
        {
            XCTFail()
            return
        }
        
        XCTAssertTrue(redis.ping())
    }

    func testSubscribe() throws
    {
        let redis = try Redis(hostname: hostname, port: port, password: nil)

        let expectation = self.expectation(description: "Subscribe")

        try redis.subscribe(channel: "ZZ1", callback: { response, _ in

            if let resp = response as? Array<RedisType> {
                switch resp[0] {
                    case let responseType as Data:
                        if responseType == "message".data {
                            switch resp[2] {
                                case let payload as Data:
                                    XCTAssertEqual(payload, "hello".data)
                                    expectation.fulfill()
                                default:
                                    XCTFail("Payload was not Data")
                            }
                        }
                    default:
                        XCTFail("Type was not Data")
                }
            }
        })

        sleep(2)
        let sent = try redis.publish(channel: "ZZ1", message: "hello")
        XCTAssertEqual(sent as? Int, 1)
        self.waitForExpectations(timeout: 10, handler: nil)
    }

    func testUnsubscribe()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)

            try redis.subscribe(channel: "ZX81", callback: { _, _ in

            })

            sleep(2)
            let receiver1 = try redis.publish(channel: "ZX81", message: "message")
            XCTAssertEqual(receiver1 as? Int, 1)

            redis.unsubscribe(channel: "ZX81")

            sleep(2)
            let receiver2 = try redis.publish(channel: "ZX81", message: "message")
            XCTAssertEqual(receiver2 as? Int, 0)
        } catch {
            XCTFail(error.localizedDescription)
        }

    }

    func testPush() throws
    {
        let redis = try Redis(hostname: hostname, port: port)
//        let authResp: Bool = try redis.auth(password: nil)
//        XCTAssertTrue(authResp)

        _ = try redis.publish(channel: "ZZ1", message: "{\"channel\":\"dd\",\"msg\":\"sss\"}")

        _ = try redis.publish(channel: "ZZ1", message: "Simple String")

        XCTAssertTrue(redis.isConnected)
        redis.close()
    }

    func testCommand() throws
    {
        let redis = try Redis(hostname: hostname, port: port)

//        let failedAuth: Bool = try redis.auth(password: "hello")
//        XCTAssertFalse(failedAuth)

//        let authResp: Bool = try redis.auth(password: nil)
//        XCTAssertTrue(authResp)

        let resultSet = try redis.set(key: "mycounter", value: "479")
        XCTAssertEqual(resultSet as? String, "OK")

        let result = try redis.get(key: "mycounter")
        XCTAssertEqual(result as? Data, "479".data)

        let unknownKey = try redis.get(key: "unknown123")
        XCTAssertNotNil(unknownKey as? NSNull)

         _ = try redis.publish(channel: "deviceID", message: "hello from swift")

        try redis.sendCommand("DEL", values: ["mylist"])
        let lpush = try redis.lpush(key: "mylist", values: "world", "mundo", "monde", "welt")
        XCTAssertEqual((lpush as? Int), 4)

        let lpopResult = try redis.lpop(key: "mylist")
        XCTAssertEqual(lpopResult as? Data, "welt".data)

        try redis.sendCommand("DEL", values: ["myset"])
        let sadd = try redis.sadd(key: "myset", values: "world", "mundo", "monde", "welt")
        XCTAssertEqual((sadd as? Int), 4)

        let smembers = try redis.smembers(key: "myset")
        XCTAssertEqual((smembers as? Array<RedisType>)?.count, 4)

        XCTAssertThrowsError(try redis.sendCommand("TTT", values: []))

        let pong = try redis.sendCommand("PING", values: [])
        XCTAssertEqual(pong.description, "PONG")

        let received = try redis.publish(channel: "ZZ1", message: "{\"channel\":\"dd\",\"msg\":\"sss\"}")
        XCTAssertEqual(received as? Int, 0)

        XCTAssertTrue(redis.isConnected)
        redis.close()
    }

    func testComplexString() {
        do
        {
            let message = "18:28:13.036 VERBOSE Extensions.init():14 - Request: Request(method: \"GET\", " +
            "path: \"/health\", body: \"\", headers: [(name: \"User-Agent\", " +
            "value: \"Paw/3.1.1 (Macintosh; OS X/10.12.5) GCDHTTPRequest\"), " +
            "(name: \"Connection\", value: \"close\"), (name: \"Host\", value: \"localhost:8000\")]), " +
            "Code: 200, Body: Host: localhost Time: 2017-06-20 16:28:13 +0000\r\n" +
            "Status: Ok Headers: [(name: \"Content-Type\", value: \"application/json\")]"

            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let push = try redis.publish(channel: "ComplexString", message: message)
            XCTAssertEqual(push as? Int, 0)
        } catch RedisError.response(let response) {
            XCTFail(response)
        } catch {
            print(error.localizedDescription)
        }
    }

    func testInitWithPassword()
    {
        XCTAssertThrowsError(try Redis(hostname: hostname, port: port, password: "Hello"))
        XCTAssertNoThrow(try Redis(hostname: hostname, port: port, password: ""))

        do {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            XCTAssertNotNil(redis)
            XCTAssertTrue(redis.isConnected)
        } catch {
            XCTFail("Init throw an error : \(error.localizedDescription)")
        }
    }

    func testClientSetName()
    {
        do {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let setName = try redis.clientSetName(clientName: "REDSHOT")
            XCTAssertEqual(setName as? String, "OK")
        } catch {
            XCTFail("Init throw an error : \(error.localizedDescription)")
        }
    }

    func testIncrement() {
        do {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let incrResult = try redis.incr(key: "INCR_KEY")
            XCTAssertEqual(incrResult as? Int, 1)
        } catch {
            XCTFail("Incr throw an error : \(error.localizedDescription)")
        }
    }

    func testSelect() {
        do {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let selectResult = try redis.select(databaseIndex: 3)
            XCTAssertEqual(selectResult as? String, "OK")
        } catch {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testllen()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let _ = try redis.lpush(key: "TEST_LIST", values: "A", "B", "C", "D")
            let _ = try redis.hset(key: "TEST_HASH", field: "MY_KEY", value: "my value")
            let llenResult = try redis.llen(key: "TEST_LIST")
            
            XCTAssertThrowsError(try redis.llen(key: "TEST_HASH"))
            XCTAssertEqual(llenResult as? Int, 4)
            
            _ = try redis.sendCommand("del", values: ["TEST_LIST", "TEST_HASH"])
        }
        catch
        {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testZadd()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let testZaddKey = "TestZaddKey"
            let testList: Array<RedisType> = ["cats", "and", "dogs", "together?!??"]
            let zaddResult = try redis.zadd(key: testZaddKey, elements: testList)
            
            XCTAssertEqual(zaddResult as? Int, testList.count)
            
            _ = try redis.sendCommand("del", values: [testZaddKey])
        }
        catch
        {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testZpopmax()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let testRSortedSetKey = "testRSortedSet1"
            let setElements = [5, 10, 15, 20, 25, 30, 35, 40]
            let _ = try redis.zadd(key: testRSortedSetKey, elements: setElements)
            let zpopmaxResult = try redis.zpopmax(key: testRSortedSetKey, count: 3)
            let resultArray = zpopmaxResult as? Array<RedisType>
            XCTAssertEqual(resultArray!.count, 6)
            _ = try redis.sendCommand("del", values: [testRSortedSetKey])
        }
        catch
        {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testZpopmin()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let testRSortedSetKey = "testRSortedSet1"
            let setElements = [5, 10, 15, 20, 25, 30, 35, 40]
            let _ = try redis.zadd(key: testRSortedSetKey, elements: setElements)
            let zpopmaxResult = try redis.zpopmin(key: testRSortedSetKey, count: 3)
            let resultArray = zpopmaxResult as? Array<RedisType>
            XCTAssertEqual(resultArray!.count, 6)
            _ = try redis.sendCommand("del", values: [testRSortedSetKey])
        }
        catch
        {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testZrangebyscore()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let testRSortedSetKey = "testRSortedSet1"
            let setElements = [5, 10, 15]
            let _ = try redis.zadd(key: testRSortedSetKey, elements: setElements)
            let rangeByScoreResult = try redis.zrangebyscore(setKey: testRSortedSetKey, minScore: 0, maxScore: 0)
            let resultArray = rangeByScoreResult as? Array<RedisType>
            XCTAssertEqual(resultArray!.count, setElements.count)
            _ = try redis.sendCommand("del", values: [testRSortedSetKey])
        }
        catch
        {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testZunionstore()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let testRSortedSet1Key = "testRSortedSet1"
            let testRSortedSet2Key = "testRSortedSet2"
            let goldenUnionKey = "testGoldenUnionSortedSet"
            let newSetKey = "testNewSetFromUnion"
            let _ = try redis.zadd(key: testRSortedSet1Key, elements: ["cats", "and", "dogs", "together?!??"])
            let _ = try redis.zadd(key: testRSortedSet2Key, elements: ["cats", "are", "never", "dogs"])
            let goldenUnionResult = try redis.zadd(key: goldenUnionKey, elements: ["cats", "are", "dogs", "and", "never", "together?!??"])
            let newSetResult = try? redis.zunionstore(newSetKey: newSetKey, firstSetKey: testRSortedSet1Key, secondSetKey: testRSortedSet2Key, firstWeight: 0.5, secondWeight: 2.0)
            XCTAssertEqual(goldenUnionResult as? Int, newSetResult as? Int)
            _ = try redis.sendCommand("del", values: [testRSortedSet1Key, testRSortedSet2Key, goldenUnionKey, newSetKey])
        }
        catch
        {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }

    func testhdel()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            
            let _ = try redis.hset(key: "TEST_HASH", field: "MY_KEY", value: "my value")
            let hdelResult = try redis.hdel(key: "TEST_HASH", field: "MY_KEY")
            
            XCTAssertEqual(hdelResult as? Int, 1)
        }
        catch
        {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testhset() {
        do {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            try redis.sendCommand("flushdb", values: [])
            let hsetResult = try redis.hset(key: "TEST_HASH", field: "MY_KEY", value: "my value")
            switch hsetResult {
                case let intResult as Int:
                    XCTAssertEqual(intResult, 1)
                default:
                    XCTFail("hset result type was not int")
            }
            let hgetResult = try redis.hget(key: "TEST_HASH", field: "MY_KEY")
            XCTAssertEqual(hgetResult as? Data, "my value".data)

            let hgetAllResult = try redis.hgetAll(key: "TEST_HASH")
            XCTAssertEqual(hgetAllResult.count, 1)

        } catch {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testhkeys()
    {
        do
        {
            let key = "TEST_HASH"
            let field = "MY_KEY"
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let _ = try redis.hset(key: key, field: "MY_KEY", value: "my value")
            let hkeysResult = try redis.hkeys(key: key)
            
            switch hkeysResult
            {
            case let arrayResult as [String]:
                XCTAssertEqual(arrayResult[0], field)
            default:
                print("HKeys result type is not a String Array: \(hkeysResult)")
            }
        }
        catch
        {
            XCTFail("HKEYS threw an error : \(error.localizedDescription)")
        }
    }

    func testBulkStringParsing()
    {
        let testValue="a\nb".data
        
        do {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            try _ = redis.set(key: "testBulkString", value: testValue, exist: nil, expire: nil)
            let result = try redis.get(key: "testBulkString")
            XCTAssertEqual(result as! Data, testValue)
        } catch {
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
    func testShutdown()
    {
        do
        {
            let redis = try Redis(hostname: hostname, port: port, password: nil)
            let result = try redis.shutdown()
            XCTAssertTrue(result)
        }
        catch let error
        {
            print("\nShutdown error")
            XCTFail("Select throw an error : \(error.localizedDescription)")
        }
    }
    
}
