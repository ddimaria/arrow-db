import React, { useState } from 'react';
import { classNames } from '../utils';

interface SchemaField {
  name: string;
  data_type: string;
  nullable: boolean;
}

interface TableSchema {
  table_name: string;
  fields: SchemaField[];
}

interface TableExplorerProps {
  tables: string[];
  schemas?: TableSchema[] | null;
  onTableSelect?: (tableName: string) => void;
  onTableDoubleClick?: (tableName: string) => void;
}

interface TableInfo {
  name: string;
  isExpanded: boolean;
}

export default function TableExplorer({
  tables,
  schemas,
  onTableSelect,
  onTableDoubleClick
}: TableExplorerProps) {
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [selectedTable, setSelectedTable] = useState<string | null>(null);

  const toggleTableExpansion = (tableName: string) => {
    const newExpanded = new Set(expandedTables);
    if (newExpanded.has(tableName)) {
      newExpanded.delete(tableName);
    } else {
      newExpanded.add(tableName);
    }
    setExpandedTables(newExpanded);
  };

  const handleTableClick = (tableName: string) => {
    setSelectedTable(tableName);
    onTableSelect?.(tableName);
  };

  const handleTableDoubleClick = (tableName: string) => {
    onTableDoubleClick?.(tableName);
  };

  // Get schema information for a specific table
  const getTableSchema = (tableName: string): SchemaField[] | null => {
    if (!schemas) return null;

    // Find the schema entry for this table
    const tableSchema = schemas.find(
      (schema) => schema.table_name.toLowerCase() === tableName.toLowerCase()
    );

    return tableSchema ? tableSchema.fields : null;
  };

  if (tables.length === 0) {
    return (
      <div className="h-full bg-gray-50 border-r border-gray-200 p-4">
        <div className="text-sm text-gray-500 text-center mt-8">
          No tables loaded
          <br />
          <span className="text-xs">Upload a parquet file to get started</span>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full bg-gray-50 border-r border-gray-200 flex flex-col">
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-200 bg-white shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900 flex items-center">
          <svg
            className="w-4 h-4 mr-2 text-blue-600"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 7v10c0 2.21 1.79 4 4 4h8c0-2.21-1.79-4-4-4H4V7z"
            />
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 7c0-2.21 1.79-4 4-4h8c2.21 0 4 1.79 4 4v10c0 2.21-1.79 4-4 4"
            />
          </svg>
          Tables
        </h2>
      </div>

      {/* Tables List */}
      <div className="flex-1 overflow-y-auto">
        <div className="p-2">
          {tables.map((tableName) => {
            const isExpanded = expandedTables.has(tableName);
            const isSelected = selectedTable === tableName;

            return (
              <div key={tableName} className="mb-1">
                {/* Table Row */}
                <div
                  className={classNames(
                    'flex items-center px-2 py-1.5 rounded cursor-pointer text-sm transition-all duration-150',
                    isSelected
                      ? 'bg-blue-100 text-blue-900 shadow-sm'
                      : 'hover:bg-white hover:shadow-sm text-gray-700'
                  )}
                  onClick={() => handleTableClick(tableName)}
                  onDoubleClick={() => handleTableDoubleClick(tableName)}
                >
                  {/* Expand/Collapse Icon */}
                  <button
                    className="mr-1 p-0.5 hover:bg-gray-200 rounded"
                    onClick={(e) => {
                      e.stopPropagation();
                      toggleTableExpansion(tableName);
                    }}
                  >
                    <svg
                      className={classNames(
                        'w-3 h-3 transition-transform',
                        isExpanded ? 'rotate-90' : 'rotate-0'
                      )}
                      fill="currentColor"
                      viewBox="0 0 20 20"
                    >
                      <path
                        fillRule="evenodd"
                        d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z"
                        clipRule="evenodd"
                      />
                    </svg>
                  </button>

                  {/* Table Icon */}
                  <svg
                    className="w-4 h-4 mr-2 text-blue-600"
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M3 10h18M3 14h18M8 4v16m8-16v16"
                    />
                  </svg>

                  {/* Table Name */}
                  <span className="flex-1 truncate font-medium">
                    {tableName}
                  </span>

                  {/* Row Count Badge (placeholder) */}
                  <span className="ml-2 px-1.5 py-0.5 text-xs bg-gray-200 text-gray-600 rounded">
                    ∞
                  </span>
                </div>

                {/* Expanded Content (Schema Info) */}
                {isExpanded && (
                  <div className="ml-6 mt-1 mb-2">
                    <div className="text-xs text-gray-500 space-y-1">
                      {(() => {
                        const tableSchema = getTableSchema(tableName);
                        if (!tableSchema) {
                          return (
                            <div className="flex items-center">
                              <svg
                                className="w-3 h-3 mr-1 text-gray-400"
                                fill="none"
                                stroke="currentColor"
                                viewBox="0 0 24 24"
                              >
                                <path
                                  strokeLinecap="round"
                                  strokeLinejoin="round"
                                  strokeWidth={2}
                                  d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
                                />
                              </svg>
                              <span>No schema information</span>
                            </div>
                          );
                        }

                        return tableSchema.map((field, index) => (
                          <div
                            key={index}
                            className="flex items-center ml-4 justify-between"
                          >
                            <div className="flex items-center">
                              <svg
                                className="w-3 h-3 mr-1 text-green-500"
                                fill="none"
                                stroke="currentColor"
                                viewBox="0 0 24 24"
                              >
                                <path
                                  strokeLinecap="round"
                                  strokeLinejoin="round"
                                  strokeWidth={2}
                                  d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
                                />
                              </svg>
                              <span className="font-mono text-gray-700 font-medium">
                                {field.name}
                              </span>
                            </div>
                            <div className="flex items-center space-x-1">
                              <span className="text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded">
                                {field.data_type}
                              </span>
                              {!field.nullable && (
                                <span className="text-xs px-1.5 py-0.5 bg-red-100 text-red-700 rounded">
                                  NOT NULL
                                </span>
                              )}
                            </div>
                          </div>
                        ));
                      })()}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Footer */}
      <div className="px-4 py-2 border-t border-gray-200 bg-white shadow-sm">
        <div className="text-xs text-gray-500 flex items-center justify-between">
          <span>
            {tables.length} table{tables.length !== 1 ? 's' : ''}
          </span>
          {tables.length > 0 && <span className="text-green-600">●</span>}
        </div>
      </div>
    </div>
  );
}
