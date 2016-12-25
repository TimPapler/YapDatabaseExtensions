//
//  Functional_ObjectWithNoMetadata.swift
//  YapDatabaseExtensions
//
//  Created by Daniel Thorpe on 13/10/2015.
//
//

import Foundation
import ValueCoding
import YapDatabase

// MARK: - Reading

extension ReadTransactionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<
        ObjectWithNoMetadata>(_ index: YapDB.Index) -> ObjectWithNoMetadata? where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return readAtIndex(index) as? ObjectWithNoMetadata
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ObjectWithNoMetadata>(_ indexes: Indexes) -> [ObjectWithNoMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return indexes.flatMap(readAtIndex)
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ObjectWithNoMetadata>(_ key: String) -> ObjectWithNoMetadata? where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return readAtIndex(ObjectWithNoMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ObjectWithNoMetadata>(_ keys: Keys) -> [ObjectWithNoMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return readAtIndexes(ObjectWithNoMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ObjectWithNoMetadata>() -> [ObjectWithNoMetadata] where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return readByKeys(keysInCollection(ObjectWithNoMetadata.collection))
    }
}

extension ConnectionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<ObjectWithNoMetadata>(_ index: YapDB.Index) -> ObjectWithNoMetadata? where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return read({ transaction -> ObjectWithNoMetadata? in
                let t:ObjectWithNoMetadata? = transaction.readAtIndex(index)
                return t
            })
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ObjectWithNoMetadata>(_ indexes: Indexes) -> [ObjectWithNoMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return read({ transaction -> [ObjectWithNoMetadata] in
                let t:[ObjectWithNoMetadata] = transaction.readAtIndexes(indexes)
                return t
            })
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ObjectWithNoMetadata>(_ key: String) -> ObjectWithNoMetadata? where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return readAtIndex(ObjectWithNoMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ObjectWithNoMetadata>(_ keys: Keys) -> [ObjectWithNoMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return readAtIndexes(ObjectWithNoMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ObjectWithNoMetadata>() -> [ObjectWithNoMetadata] where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return read({ transaction -> [ObjectWithNoMetadata] in
                let t:[ObjectWithNoMetadata] = transaction.readAll()
                return t
            })
    }
}

// MARK: - Writable

extension WriteTransactionType {

    /**
    Write the item to the database using the transaction.

    - parameter item: the item to store.
    - returns: the same item
    */
    public func write<
        ObjectWithNoMetadata>(_ item: ObjectWithNoMetadata) -> ObjectWithNoMetadata where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            writeAtIndex(item.index, object: item, metadata: .none)
            return item
    }

    /**
    Write the items to the database using the transaction.

    - parameter items: a Sequence of items to store.
    - returns: the same items, in an array.
    */
    public func write<
        Items, ObjectWithNoMetadata>(_ items: Items) -> [ObjectWithNoMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithNoMetadata,
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return items.map(write)
    }
}

extension ConnectionType {

    /**
    Write the item to the database synchronously using the connection in a new transaction.

    - parameter item: the item to store.
    - returns: the same item
    */
    public func write<
        ObjectWithNoMetadata>(_ item: ObjectWithNoMetadata) -> ObjectWithNoMetadata where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return write { $0.write(item) }
    }

    /**
    Write the items to the database synchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    - returns: the same items, in an array.
    */
    public func write<
        Items, ObjectWithNoMetadata>(_ items: Items) -> [ObjectWithNoMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithNoMetadata,
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            return write { $0.write(items) }
    }

    /**
    Write the item to the database asynchronously using the connection in a new transaction.

    - parameter item: the item to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        ObjectWithNoMetadata>(_ item: ObjectWithNoMetadata, queue: DispatchQueue = DispatchQueue.main, completion: ((ObjectWithNoMetadata) -> Void)? = .none) where
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            asyncWrite({ $0.write(item) }, queue: queue, completion: completion)
    }

    /**
    Write the items to the database asynchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        Items, ObjectWithNoMetadata>(_ items: Items, queue: DispatchQueue = DispatchQueue.main, completion: (([ObjectWithNoMetadata]) -> Void)? = .none) where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithNoMetadata,
        ObjectWithNoMetadata: Persistable,
        ObjectWithNoMetadata: NSCoding,
        ObjectWithNoMetadata.MetadataType == Void {
            asyncWrite({ $0.write(items) }, queue: queue, completion: completion)
    }
}



