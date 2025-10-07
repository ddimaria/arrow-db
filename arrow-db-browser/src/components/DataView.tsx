import React, { useState, useEffect } from 'react';
import PaginatedDataGrid from './PaginatedDataGrid';

interface PaginationInfo {
  page: number;
  page_size: number;
  rows_in_page: number;
  total_rows: number | null;
  total_pages: number | null;
  has_next_page: boolean;
  has_previous_page: boolean;
}

interface DataViewProps {
  tables: string[];
  selectedTable: string | null;
  onTableSelect: (tableName: string) => void;
  onQueryTable: (tableName: string) => Promise<string[][]>;
  onQueryTablePaginated: (
    tableName: string,
    page: number,
    pageSize: number,
    includeTotalCount: boolean
  ) => Promise<{ data: string[][]; pagination: PaginationInfo }>;
  isDatabaseReady: boolean;
}

export default function DataView({
  tables,
  selectedTable,
  onTableSelect,
  onQueryTable,
  onQueryTablePaginated,
  isDatabaseReady
}: DataViewProps) {
  const [tableData, setTableData] = useState<string[][] | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [currentPage, setCurrentPage] = useState(0);
  const [pageSize, setPageSize] = useState(100);
  const [paginationInfo, setPaginationInfo] = useState<PaginationInfo | null>(
    null
  );
  const [cachedTotalCount, setCachedTotalCount] = useState<{
    total_rows: number | null;
    total_pages: number | null;
  } | null>(null);

  // Reset pagination when table changes
  useEffect(() => {
    setCurrentPage(0);
    setPaginationInfo(null);
    setCachedTotalCount(null); // Clear cache when switching tables
  }, [selectedTable]);

  // Load data when table is selected or page changes
  useEffect(() => {
    if (selectedTable && isDatabaseReady) {
      loadTableData(selectedTable, currentPage);
    }
  }, [selectedTable, isDatabaseReady, currentPage, pageSize]);

  const loadTableData = async (tableName: string, page: number) => {
    setIsLoading(true);
    try {
      const result = await onQueryTablePaginated(
        tableName,
        page,
        pageSize,
        page === 0 // Only get total count on first page
      );
      setTableData(result.data);

      // Cache total count if available (from first page load)
      if (
        result.pagination.total_rows != null &&
        result.pagination.total_pages != null
      ) {
        setCachedTotalCount({
          total_rows: result.pagination.total_rows,
          total_pages: result.pagination.total_pages
        });
        setPaginationInfo(result.pagination);
      } else if (cachedTotalCount) {
        // Use cached values for subsequent pages
        setPaginationInfo({
          ...result.pagination,
          total_rows: cachedTotalCount.total_rows,
          total_pages: cachedTotalCount.total_pages
        });
      } else {
        setPaginationInfo(result.pagination);
      }
    } catch (error) {
      console.error('Error loading table data:', error);
      setTableData(null);
      setPaginationInfo(null);
    } finally {
      setIsLoading(false);
    }
  };

  const handlePageSizeChange = (newSize: number) => {
    setPageSize(newSize);
    setCurrentPage(0); // Reset to first page
    setCachedTotalCount(null); // Clear cache on page size change
  };

  return (
    <>
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
              <p className="text-xs text-gray-500">
                Page {currentPage + 1}
                {paginationInfo?.total_pages
                  ? ` of ${paginationInfo.total_pages}`
                  : ''}
              </p>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            {/* Refresh button */}
            <button
              onClick={() => loadTableData(selectedTable, currentPage)}
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
        </div>
      )}

      {/* Data Grid */}
      {isLoading ? (
        <div className="flex-1 bg-white flex items-center justify-center">
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
      ) : selectedTable ? (
        <PaginatedDataGrid
          data={tableData}
          paginationInfo={paginationInfo}
          isLoading={isLoading}
          currentPage={currentPage}
          pageSize={pageSize}
          onPageChange={setCurrentPage}
          onPageSizeChange={handlePageSizeChange}
          emptyMessage={{
            title: 'No data',
            description: 'This table appears to be empty'
          }}
        />
      ) : (
        <div className="flex-1 bg-white flex items-center justify-center text-gray-500">
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
    </>
  );
}
