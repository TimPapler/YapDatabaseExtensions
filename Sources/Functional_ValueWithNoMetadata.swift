//
//  Functional_ValueWithNoMetadata.swift
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
        ValueWithNoMetadata>(_ index: YapDB.Index) -> ValueWithNoMetadata? where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return ValueWithNoMetadata.decode(readAtIndex(index))
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ValueWithNoMetadata>(_ indexes: Indexes) -> [ValueWithNoMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return indexes.flatMap(readAtIndex)
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ValueWithNoMetadata>(_ key: String) -> ValueWithNoMetadata? where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return readAtIndex(ValueWithNoMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ValueWithNoMetadata>(_ keys: Keys) -> [ValueWithNoMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return readAtIndexes(ValueWithNoMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ValueWithNoMetadata>() -> [ValueWithNoMetadata] where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return readByKeys(keysInCollection(ValueWithNoMetadata.collection))
    }
}

extension ConnectionType {

    /**
    Reads the item at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType`
    */
    public func readAtIndex<
        ValueWithNoMetadata>(_ index: YapDB.Index) -> ValueWithNoMetadata? where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return read { $0.readAtIndex(index) }
    }

    /**
    Reads the items at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType`
    */
    public func readAtIndexes<
        Indexes, ValueWithNoMetadata>(_ indexes: Indexes) -> [ValueWithNoMetadata] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index,
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return read { $0.readAtIndexes(indexes) }
    }

    /**
    Reads the item at the key.

    - parameter key: a String
    - returns: an optional `ItemType`
    */
    public func readByKey<
        ValueWithNoMetadata>(_ key: String) -> ValueWithNoMetadata? where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return readAtIndex(ValueWithNoMetadata.indexWithKey(key))
    }

    /**
    Reads the items by the keys.

    - parameter keys: a Sequence of String values
    - returns: an array of `ItemType`
    */
    public func readByKeys<
        Keys, ValueWithNoMetadata>(_ keys: Keys) -> [ValueWithNoMetadata] where
        Keys: Sequence,
        Keys.Iterator.Element == String,
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return readAtIndexes(ValueWithNoMetadata.indexesWithKeys(keys))
    }

    /**
    Reads all the items in the database.

    - returns: an array of `ItemType`
    */
    public func readAll<
        ValueWithNoMetadata>() -> [ValueWithNoMetadata] where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
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
        ValueWithNoMetadata>(_ item: ValueWithNoMetadata) -> ValueWithNoMetadata where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            writeAtIndex(item.index, object: item.encoded, metadata: .none)
            return item
    }

    /**
    Write the items to the database using the transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ValueWithNoMetadata>(_ items: Items) -> [ValueWithNoMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ValueWithNoMetadata,
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return items.map(write)
    }
}

extension ConnectionType {

    /**
    Write the item to the database synchronously using the connection in a new transaction.

    - parameter item: the item to store.
    */
    public func write<
        ValueWithNoMetadata>(_ item: ValueWithNoMetadata) -> ValueWithNoMetadata where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return write { $0.write(item) }
    }

    /**
    Write the items to the database synchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    */
    public func write<
        Items, ValueWithNoMetadata>(_ items: Items) -> [ValueWithNoMetadata] where
        Items: Sequence,
        Items.Iterator.Element == ValueWithNoMetadata,
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            return write { $0.write(items) }
    }

    /**
    Write the item to the database asynchronously using the connection in a new transaction.

    - parameter item: the item to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        ValueWithNoMetadata>(_ item: ValueWithNoMetadata, queue: DispatchQueue = DispatchQueue.main, completion: ((ValueWithNoMetadata) -> Void)? = .none) where
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            asyncWrite({ $0.write(item) }, queue: queue, completion: completion)
    }

    /**
    Write the items to the database asynchronously using the connection in a new transaction.

    - parameter items: a Sequence of items to store.
    - parameter queue: the dispatch_queue_t to run the completion block on.
    - parameter completion: a dispatch_block_t for completion.
    */
    public func asyncWrite<
        Items, ValueWithNoMetadata>(_ items: Items, queue: DispatchQueue = DispatchQueue.main, completion: (([ValueWithNoMetadata]) -> Void)? = .none) where
        Items: Sequence,
        Items.Iterator.Element == ValueWithNoMetadata,
        ValueWithNoMetadata: Persistable,
        ValueWithNoMetadata: ValueCoding,
        ValueWithNoMetadata.Coder: NSCoding,
        ValueWithNoMetadata.Coder.Value == ValueWithNoMetadata,
        ValueWithNoMetadata.MetadataType == Void {
            asyncWrite({ $0.write(items) }, queue: queue, completion: completion)
    }
}

