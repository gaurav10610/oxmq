package io.oxmq.oxdoc.model;

/**
 * Content intelligence indexing verdict issued by the document extraction engine.
 */
public enum IndexingVerdict {
    INDEXED_SEARCHABLE,      // Full text extracted, indexed and searchable
    STRUCTURED_DOCUMENT,     // High-density structured financial/data model or spreadsheet
    BINARY_ARCHIVED,         // Raw binary, compressed archive, or dataset
    PARSED_METADATA_ONLY     // Document metadata parsed without full-text payload
}
