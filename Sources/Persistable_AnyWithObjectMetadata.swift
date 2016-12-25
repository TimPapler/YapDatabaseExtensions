//
//  Persistable_AnyWithObjectMetadata.swift
//  YapDatabaseExtensions
//
//  Created by Daniel Thorpe on 13/10/2015.
//
//

import Foundation
import ValueCoding

// MARK: - Readable

extension Readable where
    ItemType: Persistable,
    ItemType.MetadataType: NSCoding {

    func metadataInTransaction(_ transaction: Database.Connection.ReadTransaction, atIndex index: YapDB.Index) -> ItemType.MetadataType? {
        return transaction.readMetadataAtIndex(index)
    }

    func metadataAtIndexInTransaction(_ index: YapDB.Index) -> (Database.Connection.ReadTransaction) -> ItemType.MetadataType? {
        return { self.metadataInTransaction($0, atIndex: index) }
    }

    func metadataInTransactionAtIndex(_ transaction: Database.Connection.ReadTransaction) -> (YapDB.Index) -> ItemType.MetadataType? {
        return { self.metadataInTransaction(transaction, atIndex: $0) }
    }

    func metadataAtIndexesInTransaction<
        Indexes>(_ indexes: Indexes) -> (Database.Connection.ReadTransaction) -> [ItemType.MetadataType] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index {
            let atIndex = metadataInTransactionAtIndex
            return { indexes.flatMap(atIndex($0)) }
    }

    /**
    Reads the metadata at a given index.

    - parameter index: a YapDB.Index
    - returns: an optional `ItemType.MetadataType`
    */
    public func metadataAtIndex(_ index: YapDB.Index) -> ItemType.MetadataType? {
        return sync(metadataAtIndexInTransaction(index))
    }

    /**
    Reads the metadata at the indexes.

    - parameter indexes: a Sequence of YapDB.Index values
    - returns: an array of `ItemType.MetadataType`
    */
    public func metadataAtIndexes<
        Indexes>(_ indexes: Indexes) -> [ItemType.MetadataType] where
        Indexes: Sequence,
        Indexes.Iterator.Element == YapDB.Index {
            return sync(metadataAtIndexesInTransaction(indexes))
    }
}
