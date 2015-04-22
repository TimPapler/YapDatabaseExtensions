//
//  Created by Daniel Thorpe on 22/04/2015.
//  Copyright (c) 2015 Daniel Thorpe. All rights reserved.
//

import UIKit
import XCTest
import YapDatabase
import YapDatabaseExtensions
import YapDBExtensionsMobile

class ReadWriteTests: XCTestCase {

    let person = Person(id: "user-123", name: "Robbie")
    let barcode: Barcode = .QRCode("I have no idea what the string of a QR Code might look like")
    let product = Product(metadata: Product.Metadata(categoryIdentifier: 1), identifier: "cocoa-123", name: "CocoaPops", barcode: .UPCA(1, 2, 3, 4))

    func people() -> [Person] {
        return [
            Person(id: "beatle-1", name: "John"),
            Person(id: "beatle-2", name: "Paul"),
            Person(id: "beatle-3", name: "George"),
            Person(id: "beatle-4", name: "Ringo")
        ]
    }

    func barcodes() -> Set<Barcode> {
        return [
            .QRCode("I have no idea what the string of a QR Code might look like"),
            .QRCode("This could honestly be what it looks like."),
            .UPCA(1, 2, 3, 5),
            .UPCA(8, 13, 21, 34)
        ]
    }
}

extension ReadWriteTests {

    func testSynchronous_ReadingAndWriting_Object() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        validateWrite(db.write(person), person, usingDatabase: db)
    }

    func testSynchronous_ReadingAndWriting_Value() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        validateWrite(db.write(barcode), barcode, usingDatabase: db)
    }

    func testSynchronous_ReadingAndWriting_ValueWithValueMetadata() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        validateWrite(db.write(product), product, usingDatabase: db)
    }
}

extension ReadWriteTests {

    func testSynchronous_ReadingAndWriting_ManyObjects() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        let objects = people()

        db.write(objects)
        let read: [Person] = db.readAll()
        XCTAssertEqual(objects, read, "Expecting all keys in collection to return all items.")
    }

    func testSynchronous_ReadingAndWriting_ManyValues() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        let values = barcodes()

        db.write(values)
        let read: Set<Barcode> = Set(db.readAll())
        XCTAssertEqual(values, read, "Expecting all keys in collection to return all items.")
    }
}

extension ReadWriteTests {

    func testAsynchronous_ReadingAndWriting_Object() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        let expectation = expectationWithDescription("Finished async writing of object.")

        db.write(person) {
            validateWrite($0, self.person, usingDatabase: db)
            expectation.fulfill()
        }

        waitForExpectationsWithTimeout(5.0, handler: nil)
    }

    func testAsynchronous_ReadingAndWriting_Value() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        let expectation = expectationWithDescription("Finished async writing of value.")

        db.write(barcode) {
            validateWrite($0, self.barcode, usingDatabase: db)
            expectation.fulfill()
        }

        waitForExpectationsWithTimeout(5.0, handler: nil)
    }

    func testAsynchronous_ReadingAndWriting_ValueWithValueMetadata() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)
        let expectation = expectationWithDescription("Finished async writing of value with value metadata.")

        db.write(product) {
            validateWrite($0, self.product, usingDatabase: db)
            expectation.fulfill()
        }

        waitForExpectationsWithTimeout(5.0, handler: nil)
    }
}

extension ReadWriteTests {

    func testSynchronous_RemoveAtIndex() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)

        db.write(barcode)
        XCTAssertEqual((db.readAll() as [Barcode]).count, 1, "There should be one barcodes in the database.")

        db.removeAtIndex(indexForPersistable(barcode))
        XCTAssertEqual((db.readAll() as [Barcode]).count, 0, "There should be no barcodes in the database.")
    }

/* - For some reason this doesn't link...
    func testSynchronous_RemoveAtIndexes() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)

        let _barcodes = barcodes()
        db.write(_barcodes)
        XCTAssertEqual((db.readAll() as [Barcode]).count, _barcodes.count, "There should be one barcodes in the database.")

        let indexes: [YapDatabase.Index] = map(_barcodes, indexForPersistable)
        db.removeAtIndexes(indexes)
        XCTAssertEqual((db.readAll() as [Barcode]).count, 0, "There should be no barcodes in the database.")
    }
*/
}

extension ReadWriteTests {

    func testSynchronous_RemovePersistable() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)

        db.write(barcode)
        XCTAssertEqual((db.readAll() as [Barcode]).count, 1, "There should be one barcodes in the database.")

        db.remove(barcode)
        XCTAssertEqual((db.readAll() as [Barcode]).count, 0, "There should be no barcodes in the database.")
    }

    func testSynchronous_RemovePersistables() {
        let db = createYapDatabase(__FILE__, suffix: __FUNCTION__)

        let _barcodes = barcodes()
        db.write(_barcodes)
        XCTAssertEqual((db.readAll() as [Barcode]).count, _barcodes.count, "There should be one barcodes in the database.")

        db.remove(_barcodes)
        XCTAssertEqual((db.readAll() as [Barcode]).count, 0, "There should be no barcodes in the database.")
    }
}



