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
        ValueWithValueMetadata>(_ index: YapDB.Index) -> ValueWithValueMetadata? where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            if var item = ValueWithValueMetadata.decode(readAtIndex(index)) {
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
        Indexes, ValueWithValueMetadata>(_ indexes: Indexes) -> [ValueWithValueMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return indexes.flatMap(readAtIndex)
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ValueWithValueMetadata>(_ key: String) -> ValueWithValueMetadata? where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return readAtIndex(ValueWithValueMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ValueWithValueMetadata>(_ keys: Keys) -> [ValueWithValueMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return readAtIndexes(ValueWithValueMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ValueWithValueMetadata>() -> [ValueWithValueMetadata] where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return readByKeys(keysInCollection(ValueWithValueMetadata.collection))
    }
}

extension ConnectionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<
        ValueWithValueMetadata>(_ index: YapDB.Index) -> ValueWithValueMetadata? where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return read { $0.readAtIndex(index) }
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ValueWithValueMetadata>(_ indexes: Indexes) -> [ValueWithValueMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return read { $0.readAtIndexes(indexes) }
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ValueWithValueMetadata>(_ key: String) -> ValueWithValueMetadata? where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return readAtIndex(ValueWithValueMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ValueWithValueMetadata>(_ keys: Keys) -> [ValueWithValueMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return readAtIndexes(ValueWithValueMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ValueWithValueMetadata>() -> [ValueWithValueMetadata] where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return read { $0.readAll() }
    }
}

// MARK: - Writing

extension WriteTransactionType {

    /**
    Write the item to the database using the transaction.

    - parameter item: the item to store.
    */
    public func write<
        ValueWithValueMetadata>(_ item: ValueWithValueMetadata) -> ValueWithValueMetadata where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            writeAtIndex(item.index, object: item.encoded, metadata: item.metadata?.encoded)
            return item
    }

    /**
    Write the items to the database using the transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ValueWithValueMetadata>(_ items: Items) -> [ValueWithValueMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ValueWithValueMetadata,
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return items.map(write)
    }
}

extension ConnectionType {

    /**
    Write the item to the database synchronously using the connection in a new transaction.

    - parameter item: the item to store.
    */
    public func write<
        ValueWithValueMetadata>(_ item: ValueWithValueMetadata) -> ValueWithValueMetadata where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return write { $0.write(item) }
    }

    /**
    Write the items to the database synchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ValueWithValueMetadata>(_ items: Items) -> [ValueWithValueMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ValueWithValueMetadata,
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            return write { $0.write(items) }
    }

    /**
    Write the item to the database asynchronously using the connection in a new transaction.

    - parameter item: the item to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        ValueWithValueMetadata>(_ item: ValueWithValueMetadata, queue: DispatchQueue = DispatchQueue.main, completion: ((ValueWithValueMetadata) -> Void)? = .none) where
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            asyncWrite({ $0.write(item) }, queue: queue, completion: completion)
    }

    /**
    Write the items to the database asynchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        Items, ValueWithValueMetadata>(_ items: Items, queue: DispatchQueue = DispatchQueue.main, completion: (([ValueWithValueMetadata]) -> Void)? = .none) where
        Items: Sequence,
        Items.Iterator.Element == ValueWithValueMetadata,
        ValueWithValueMetadata: Persistable,
        ValueWithValueMetadata: ValueCoding,
        ValueWithValueMetadata.Coder: NSCoding,
        ValueWithValueMetadata.Coder.Value == ValueWithValueMetadata,
        ValueWithValueMetadata.MetadataType: ValueCoding,
        ValueWithValueMetadata.MetadataType.Coder: NSCoding,
        ValueWithValueMetadata.MetadataType.Coder.Value == ValueWithValueMetadata.MetadataType {
            asyncWrite({ $0.write(items) }, queue: queue, completion: completion)
    }
}


