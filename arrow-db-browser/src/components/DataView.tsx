import React, { useState, useEffect, useMemo } from 'react';
import DataGrid, { Column, RenderCellProps } from 'react-data-grid';

interface Cell {
  id: string;
  title: string[];
}
type Row = Cell;

interface DataViewProps {
  tables: string[];
  selectedTable: string | null;
  onTableSelect: (tableName: string) => void;
  onQueryTable: (tableName: string) => Promise<string[][]>;
  isDatabaseReady: boolean;
}

export default function DataView({
  tables,
  selectedTable,
  onTableSelect,
  onQueryTable,
  isDatabaseReady
}: DataViewProps) {
  const [tableData, setTableData] = useState<string[][] | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  // Load data when table is selected
  useEffect(() => {
    if (selectedTable && isDatabaseReady) {
      loadTableData(selectedTable);
    }
  }, [selectedTable, isDatabaseReady]);

  const loadTableData = async (tableName: string) => {
    setIsLoading(true);
    try {
      const data = await onQueryTable(tableName);
      setTableData(data);
    } catch (error) {
      console.error('Error loading table data:', error);
      setTableData(null);
    } finally {
      setIsLoading(false);
    }
  };

  const columns = useMemo((): readonly Column<any>[] => {
    let columns: Column<any>[] = [];

    if (tableData && tableData.length > 0) {
      columns = tableData[0].map((header, index) => ({
        key: String(index),
        name: header,
        width: 150,
        minWidth: 100,
        resizable: true,
        renderCell: (props: RenderCellProps<Cell>) =>
          `${props.row.title[index]}`
      }));
    }

    return columns;
  }, [tableData]);

  const rows = useMemo((): readonly Row[] => {
    let rows: Row[] = [];

    if (tableData && tableData.length > 1) {
      for (let i = 1; i < tableData.length; i++) {
        rows.push({
          id: String(i),
          title: tableData[i]
        });
      }
    }

    return rows;
  }, [tableData]);

  return (
    <div className="flex-1 flex flex-col">
      {/* Table Info Header */}
      {selectedTable && (
        <div className="bg-white border-b border-gray-200 px-4 py-3 flex items-center justify-between">
          <div className="flex items-center space-x-3">
            <svg
              className="w-5 h-5 text-blue-600"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z"
              />
            </svg>
            <div>
              <h3 className="text-sm font-semibold text-gray-900">
                {selectedTable}
              </h3>
              <p className="text-xs text-gray-500">Viewing all data</p>
            </div>
          </div>
          <button
            onClick={() => loadTableData(selectedTable)}
            disabled={isLoading}
            className="flex items-center space-x-2 px-3 py-1.5 text-sm text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-md disabled:text-gray-400 disabled:hover:bg-transparent transition-colors"
          >
            <svg
              className="w-4 h-4"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
              />
            </svg>
            <span>Refresh</span>
          </button>
        </div>
      )}

      {/* Data Grid */}
      <div className="flex-1 bg-white overflow-hidden relative">
        {isLoading ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <svg
                className="animate-spin h-8 w-8 text-blue-500 mx-auto mb-2"
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
              >
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                ></circle>
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              <p className="text-sm text-gray-600">Loading table data...</p>
            </div>
          </div>
        ) : tableData && tableData.length > 0 ? (
          <>
            <DataGrid
              columns={columns}
              rows={rows}
              rowHeight={35}
              className="fill-grid"
              direction="ltr"
              enableVirtualization={true}
              rowKeyGetter={(row) => row.id}
            />
            {/* Row count indicator */}
            <div className="absolute bottom-0 right-0 bg-white border-t border-l border-gray-200 px-3 py-1 text-xs text-gray-600">
              {rows.length} row{rows.length !== 1 ? 's' : ''}
            </div>
          </>
        ) : selectedTable ? (
          <div className="flex items-center justify-center h-full text-gray-500">
            <div className="text-center">
              <svg
                className="mx-auto h-12 w-12 text-gray-400 mb-4"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"
                />
              </svg>
              <p className="text-lg font-medium text-gray-900 mb-1">No data</p>
              <p className="text-sm text-gray-500">
                This table appears to be empty
              </p>
            </div>
          </div>
        ) : (
          <div className="flex items-center justify-center h-full text-gray-500">
            <div className="text-center">
              <svg
                className="mx-auto h-12 w-12 text-gray-400 mb-4"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M3 10h18M3 14h18m-9-4v8m-7 0h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z"
                />
              </svg>
              <p className="text-lg font-medium text-gray-900 mb-1">
                Select a table
              </p>
              <p className="text-sm text-gray-500">
                Click on a table in the left panel to view its data
              </p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
