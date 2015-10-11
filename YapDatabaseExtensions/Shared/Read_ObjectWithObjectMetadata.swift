//
//  Read_ObjectWithObjectMetadata.swift
//  YapDatabaseExtensions
//
//  Created by Daniel Thorpe on 11/10/2015.
//
//

import Foundation
import ValueCoding
import YapDatabase

// MARK: - Object with Object metadata

extension Readable
    where
    ItemType: NSCoding,
    ItemType: MetadataPersistable,
    ItemType.MetadataType: NSCoding {

    func inTransaction(transaction: Database.Connection.ReadTransaction, atIndex index: YapDB.Index) -> ItemType? {
        return transaction.readAtIndex(index)
    }

    // Everything here is the same for all 6 patterns.

    func inTransactionAtIndex(transaction: Database.Connection.ReadTransaction) -> YapDB.Index -> ItemType? {
        return { self.inTransaction(transaction, atIndex: $0) }
    }

    func atIndexInTransaction(index: YapDB.Index) -> Database.Connection.ReadTransaction -> ItemType? {
        return { self.inTransaction($0, atIndex: index) }
    }

    func atIndexesInTransaction(indexes: [YapDB.Index]) -> Database.Connection.ReadTransaction -> [ItemType] {
        let atIndex = inTransactionAtIndex
        return { transaction in
            indexes.flatMap(atIndex(transaction))
        }
    }

    func inTransaction(transaction: Database.Connection.ReadTransaction, byKey key: String) -> ItemType? {
        return inTransaction(transaction, atIndex: ItemType.indexWithKey(key))
    }

    func inTransactionByKey(transaction: Database.Connection.ReadTransaction) -> String -> ItemType? {
        return { self.inTransaction(transaction, byKey: $0) }
    }

    func byKeyInTransaction(key: String) -> Database.Connection.ReadTransaction -> ItemType? {
        return { self.inTransaction($0, byKey: key) }
    }

    func byKeysInTransaction(_keys: [String]? = .None) -> Database.Connection.ReadTransaction -> [ItemType] {
        let byKey = inTransactionByKey
        return { transaction in
            let keys = _keys ?? transaction.keysInCollection(ItemType.collection)
            return keys.flatMap(byKey(transaction))
        }
    }

    public func atIndex(index: YapDB.Index) -> ItemType? {
        return sync(atIndexInTransaction(index))
    }

    public func atIndexes(indexes: [YapDB.Index]) -> [ItemType] {
        return sync(atIndexesInTransaction(indexes))
    }

    public func byKey(key: String) -> ItemType? {
        return sync(byKeyInTransaction(key))
    }

    public func byKeys(keys: [String]) -> [ItemType] {
        return sync(byKeysInTransaction(keys))
    }

    public func all() -> [ItemType] {
        return sync(byKeysInTransaction())
    }

    public func filterExisting(keys: [String]) -> (existing: [ItemType], missing: [String]) {
        let existingInTransaction = byKeysInTransaction(keys)
        return sync { transaction -> ([ItemType], [String]) in
            let existing = existingInTransaction(transaction)
            let existingKeys = existing.map(keyForPersistable)
            let missingKeys = keys.filter { !existingKeys.contains($0) }
            return (existing, missingKeys)
        }
    }
}

// MARK: - Object with Object metadata

extension ReadTransactionType {

    public func readAtIndex<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(index: YapDB.Index) -> ObjectWithObjectMetadata? {
            if var item = readAtIndex(index) as? ObjectWithObjectMetadata {
                item.metadata = readMetadataAtIndex(index) as? ObjectWithObjectMetadata.MetadataType
                return item
            }
            return .None
    }

    public func readAtIndexes<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(indexes: [YapDB.Index]) -> [ObjectWithObjectMetadata] {
            return indexes.flatMap(readAtIndex)
    }

    public func readByKey<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(key: String) -> ObjectWithObjectMetadata? {
            return readAtIndex(ObjectWithObjectMetadata.indexWithKey(key))
    }

    public func readByKeys<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(keys: [String]) -> [ObjectWithObjectMetadata] {
            return readAtIndexes(ObjectWithObjectMetadata.indexesWithKeys(keys))
    }
}

extension ConnectionType {

    public func readAtIndex<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(index: YapDB.Index) -> ObjectWithObjectMetadata? {
            return read { $0.readAtIndex(index) }
    }

    public func readAtIndexes<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(indexes: [YapDB.Index]) -> [ObjectWithObjectMetadata] {
            return read { $0.readAtIndexes(indexes) }
    }

    public func readByKey<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(key: String) -> ObjectWithObjectMetadata? {
            return readAtIndex(ObjectWithObjectMetadata.indexWithKey(key))
    }

    public func readByKeys<
        ObjectWithObjectMetadata
        where
        ObjectWithObjectMetadata: MetadataPersistable,
        ObjectWithObjectMetadata: NSCoding,
        ObjectWithObjectMetadata.MetadataType: NSCoding>(keys: [String]) -> [ObjectWithObjectMetadata] {
            return readAtIndexes(ObjectWithObjectMetadata.indexesWithKeys(keys))
    }
}

