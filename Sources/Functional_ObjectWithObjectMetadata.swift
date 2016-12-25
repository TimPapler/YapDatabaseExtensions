//
//  Functional_ObjectWithNoMetadata.swift
//  YapDatabaseExtensions
//
//  Created by Daniel Thorpe on 11/10/2015.
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
        ObjectWithObjectMetadata>(_ index: YapDB.Index) -> ObjectWithObjectMetadata? where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            if var item = readAtIndex(index) as? ObjectWithObjectMetadata {
                item.metadata = readMetadataAtIndex(index)
                return item
            }
            return .none
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ObjectWithObjectMetadata>(_ indexes: Indexes) -> [ObjectWithObjectMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return indexes.flatMap(readAtIndex)
    }

    /**
    Reads the item by key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ObjectWithObjectMetadata>(_ key: String) -> ObjectWithObjectMetadata? where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndex(ObjectWithObjectMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ObjectWithObjectMetadata>(_ keys: Keys) -> [ObjectWithObjectMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndexes(ObjectWithObjectMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ObjectWithObjectMetadata>() -> [ObjectWithObjectMetadata] where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return readByKeys(keysInCollection(ObjectWithObjectMetadata.collection))
    }
}

extension ConnectionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<
        ObjectWithObjectMetadata>(_ index: YapDB.Index) -> ObjectWithObjectMetadata? where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return read { $0.readAtIndex(index) }
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ObjectWithObjectMetadata>(_ indexes: Indexes) -> [ObjectWithObjectMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return read { $0.readAtIndexes(indexes) }
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ObjectWithObjectMetadata>(_ key: String) -> ObjectWithObjectMetadata? where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndex(ObjectWithObjectMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ObjectWithObjectMetadata>(_ keys: Keys) -> [ObjectWithObjectMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return readAtIndexes(ObjectWithObjectMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ObjectWithObjectMetadata>() -> [ObjectWithObjectMetadata] where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return read { $0.readAll() }
    }
}

// MARK: - Writable

extension WriteTransactionType {

    /**
    Write the item to the database using the transaction.

    - parameter item: the item to store.
    */
    public func write<
        ObjectWithObjectMetadata>(_ item: ObjectWithObjectMetadata) -> ObjectWithObjectMetadata where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            writeAtIndex(item.index, object: item, metadata: item.metadata)
            return item
    }

    /**
    Write the items to the database using the transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ObjectWithObjectMetadata>(_ items: Items) -> [ObjectWithObjectMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithObjectMetadata,
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return items.map(write)
    }
}

extension ConnectionType {

    /**
    Write the item to the database synchronously using the connection in a new transaction.

    - parameter item: the item to store.
    */
    public func write<
        ObjectWithObjectMetadata>(_ item: ObjectWithObjectMetadata) -> ObjectWithObjectMetadata where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return write { $0.write(item) }
    }

    /**
    Write the items to the database synchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ObjectWithObjectMetadata>(_ items: Items) -> [ObjectWithObjectMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithObjectMetadata,
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            return write { $0.write(items) }
    }

    /**
    Write the item to the database asynchronously using the connection in a new transaction.

    - parameter item: the item to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        ObjectWithObjectMetadata>(_ item: ObjectWithObjectMetadata, queue: DispatchQueue = DispatchQueue.main, completion: ((ObjectWithObjectMetadata) -> Void)? = .none) where
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            asyncWrite({ $0.write(item) }, queue: queue, completion: completion)
    }

    /**
    Write the items to the database asynchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        Items, ObjectWithObjectMetadata>(_ items: Items, queue: DispatchQueue = DispatchQueue.main, completion: (([ObjectWithObjectMetadata]) -> Void)? = .none) where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithObjectMetadata,
        ObjectWithObjectMetadata: Persistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding {
            asyncWrite({ $0.write(items) }, queue: queue, completion: completion)
    }
}



