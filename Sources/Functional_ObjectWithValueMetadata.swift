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
        ObjectWithValueMetadata>(_ index: YapDB.Index) -> ObjectWithValueMetadata? where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            if var item = readAtIndex(index) as? ObjectWithValueMetadata {
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
        Indexes, ObjectWithValueMetadata>(_ indexes: Indexes) -> [ObjectWithValueMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return indexes.flatMap(readAtIndex)
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ObjectWithValueMetadata>(_ key: String) -> ObjectWithValueMetadata? where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return readAtIndex(ObjectWithValueMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ObjectWithValueMetadata>(_ keys: Keys) -> [ObjectWithValueMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return readAtIndexes(ObjectWithValueMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ObjectWithValueMetadata>() -> [ObjectWithValueMetadata] where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return readByKeys(keysInCollection(ObjectWithValueMetadata.collection))
    }
}

extension ConnectionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<
        ObjectWithValueMetadata>(_ index: YapDB.Index) -> ObjectWithValueMetadata? where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return read { $0.readAtIndex(index) }
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ObjectWithValueMetadata>(_ indexes: Indexes) -> [ObjectWithValueMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return read { $0.readAtIndexes(indexes) }
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ObjectWithValueMetadata>(_ key: String) -> ObjectWithValueMetadata? where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return readAtIndex(ObjectWithValueMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ObjectWithValueMetadata>(_ keys: Keys) -> [ObjectWithValueMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return readAtIndexes(ObjectWithValueMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ObjectWithValueMetadata>() -> [ObjectWithValueMetadata] where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return read { $0.readAll() } 
    }
}

// MARK: - Reading

extension WriteTransactionType {

    /**
    Write the item to the database using the transaction.

    - parameter item: the item to store.
    */
    public func write<
        ObjectWithValueMetadata>(_ item: ObjectWithValueMetadata) -> ObjectWithValueMetadata where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            writeAtIndex(item.index, object: item, metadata: item.metadata?.encoded)
            return item
    }

    /**
    Write the items to the database using the transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ObjectWithValueMetadata>(_ items: Items) -> [ObjectWithValueMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithValueMetadata,
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return items.map(write)
    }
}

extension ConnectionType {

    /**
    Write the item to the database synchronously using the connection in a new transaction.

    - parameter item: the item to store.
    */
    public func write<
        ObjectWithValueMetadata>(_ item: ObjectWithValueMetadata) -> ObjectWithValueMetadata where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return write { $0.write(item) }
    }

    /**
    Write the items to the database synchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ObjectWithValueMetadata>(_ items: Items) -> [ObjectWithValueMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithValueMetadata,
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            return write { $0.write(items) }
    }

    /**
    Write the item to the database asynchronously using the connection in a new transaction.

    - parameter item: the item to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        ObjectWithValueMetadata>(_ item: ObjectWithValueMetadata, queue: DispatchQueue = DispatchQueue.main, completion: ((ObjectWithValueMetadata) -> Void)? = .none) where
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            asyncWrite({ $0.write(item) }, queue: queue, completion: completion)
    }

    /**
    Write the items to the database asynchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        Items, ObjectWithValueMetadata>(_ items: Items, queue: DispatchQueue = DispatchQueue.main, completion: (([ObjectWithValueMetadata]) -> Void)? = .none) where
        Items: Sequence,
        Items.Iterator.Element == ObjectWithValueMetadata,
        ObjectWithValueMetadata: Persistable,
        ObjectWithValueMetadata: NSCoding,
        ObjectWithValueMetadata.MetadataType: ValueCoding,
        ObjectWithValueMetadata.MetadataType.Coder: NSCoding,
        ObjectWithValueMetadata.MetadataType.Coder.Value == ObjectWithValueMetadata.MetadataType {
            asyncWrite({ $0.write(items) }, queue: queue, completion: completion)
    }
}


