import React, { useEffect } from 'react';
import SqlEditor from './SqlEditor';
import PaginatedDataGrid from './PaginatedDataGrid';
//@ts-ignore
import type { ArrowDbWasm } from './../../arrow-db-wasm';

interface PaginationInfo {
  page: number;
  page_size: number;
  rows_in_page: number;
  total_rows: number | null;
  total_pages: number | null;
  has_next_page: boolean;
  has_previous_page: boolean;
}

interface SchemaField {
  name: string;
  data_type: string;
  nullable: boolean;
}

interface TableSchema {
  table_name: string;
  fields: SchemaField[];
}

interface SqlViewProps {
  database: ArrowDbWasm;
  isDatabaseReady: boolean;
  query: string;
  onQueryChange: (query: string) => void;
  onShowAlert: (
    title: string,
    message: string,
    type: 'danger' | 'warning' | 'info' | 'success'
  ) => void;
  schemas: TableSchema[] | null;
  // State managed in App.tsx for persistence
  output: string[][] | null;
  setOutput: (output: string[][] | null) => void;
  paginationInfo: PaginationInfo | null;
  setPaginationInfo: (info: PaginationInfo | null) => void;
  cachedTotalCount: {
    total_rows: number | null;
    total_pages: number | null;
  } | null;
  setCachedTotalCount: (
    count: { total_rows: number | null; total_pages: number | null } | null
  ) => void;
  currentPage: number;
  setCurrentPage: (page: number) => void;
  pageSize: number;
  setPageSize: (size: number) => void;
  isQueryLoading: boolean;
  setIsQueryLoading: (loading: boolean) => void;
}

export default function SqlView({
  database,
  isDatabaseReady,
  query,
  onQueryChange,
  onShowAlert,
  schemas,
  output,
  setOutput,
  paginationInfo,
  setPaginationInfo,
  cachedTotalCount,
  setCachedTotalCount,
  currentPage,
  setCurrentPage,
  pageSize,
  setPageSize,
  isQueryLoading,
  setIsQueryLoading
}: SqlViewProps) {
  // Re-run query when page changes
  useEffect(() => {
    if (query && output) {
      handleQuery(false);
    }
  }, [currentPage, pageSize]);

  const handleQuery = (resetPage = false) => {
    if (query !== '') {
      console.log('Executing query:', query);
      console.log('Database ready:', isDatabaseReady);
      console.log('Database instance:', database);

      if (!isDatabaseReady || !database) {
        console.error('Database not ready yet');
        onShowAlert(
          'Database Not Ready',
          'Database not ready. Please wait a moment and try again.',
          'warning'
        );
        return;
      }

      if (resetPage) {
        setCurrentPage(0);
        setCachedTotalCount(null); // Clear cache on new query
        // Clear previous results only on new query
        setOutput(null);
        setPaginationInfo(null);
      }

      // Show loading state
      setIsQueryLoading(true);

      // Use MessageChannel to defer execution and allow React to render loading state
      const channel = new MessageChannel();
      channel.port2.onmessage = () => {
        database
          .query_paginated(
            query,
            resetPage ? 0 : currentPage,
            pageSize,
            resetPage ? true : currentPage === 0
          )
          .then((results: any) => {
            if (
              results &&
              results.data &&
              results.data[0] &&
              results.data[0].data
            ) {
              setOutput(results.data[0].data);

              // Cache total count if available (from first page load)
              if (
                results.pagination.total_rows != null &&
                results.pagination.total_pages != null
              ) {
                setCachedTotalCount({
                  total_rows: results.pagination.total_rows,
                  total_pages: results.pagination.total_pages
                });
                setPaginationInfo(results.pagination);
              } else if (cachedTotalCount) {
                // Use cached values for subsequent pages
                setPaginationInfo({
                  ...results.pagination,
                  total_rows: cachedTotalCount.total_rows,
                  total_pages: cachedTotalCount.total_pages
                });
              } else {
                setPaginationInfo(results.pagination);
              }
            } else {
              console.error(
                'Unexpected paginated query result format:',
                results
              );
              onShowAlert(
                'Query Error',
                'Query executed but returned unexpected format',
                'warning'
              );
            }
          })
          .catch((error: any) => {
            console.error('Query error:', error);
            onShowAlert(
              'Query Failed',
              `Query failed: ${error.message || error}`,
              'danger'
            );
          })
          .finally(() => {
            setIsQueryLoading(false);
          });
      };
      channel.port1.postMessage(null);
    } else {
      onShowAlert('No Query', 'Please enter a query', 'info');
    }
  };

  return (
    <>
      <div className="bg-white border-b border-gray-200 p-4">
        <div className="flex items-center justify-between mb-2">
          <label className="text-sm font-medium text-gray-700">SQL Query</label>
          <button
            className={`px-4 py-2 text-white text-sm font-medium rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 flex items-center space-x-2 ${
              isDatabaseReady && !isQueryLoading
                ? 'bg-blue-600 hover:bg-blue-700'
                : 'bg-gray-400 cursor-not-allowed'
            }`}
            onClick={() => handleQuery(true)}
            disabled={!isDatabaseReady || isQueryLoading}
          >
            {isQueryLoading && (
              <svg
                className="animate-spin -ml-1 mr-2 h-4 w-4 text-white"
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
            )}
            <span>
              {isQueryLoading
                ? 'Running...'
                : isDatabaseReady
                  ? 'Run Query'
                  : 'Loading...'}
            </span>
          </button>
        </div>
        <SqlEditor
          value={query}
          onChange={onQueryChange}
          schemas={schemas}
          isLoading={isQueryLoading}
          isDatabaseReady={isDatabaseReady}
          onRunQuery={() => handleQuery(true)}
        />
      </div>

      {/* Results Grid */}
      <PaginatedDataGrid
        data={output}
        paginationInfo={paginationInfo}
        isLoading={isQueryLoading}
        currentPage={currentPage}
        pageSize={pageSize}
        onPageChange={setCurrentPage}
        onPageSizeChange={(size) => {
          setPageSize(size);
          setCurrentPage(0);
          setCachedTotalCount(null); // Clear cache on page size change
        }}
        emptyMessage={{
          title: 'No query results',
          description: 'Run a SQL query to see results here'
        }}
      />
    </>
  );
}
