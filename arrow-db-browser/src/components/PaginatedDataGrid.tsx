import React, { useMemo } from 'react';
import DataGrid, { Column, RenderCellProps } from 'react-data-grid';

interface Cell {
  id: string;
  title: string[];
}
type Row = Cell;

interface PaginationInfo {
  page: number;
  page_size: number;
  rows_in_page: number;
  total_rows: number | null;
  total_pages: number | null;
  has_next_page: boolean;
  has_previous_page: boolean;
}

interface PaginatedDataGridProps {
  data: string[][] | null;
  paginationInfo: PaginationInfo | null;
  isLoading?: boolean;
  currentPage: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
  emptyMessage?: {
    title: string;
    description: string;
  };
}

export default function PaginatedDataGrid({
  data,
  paginationInfo,
  isLoading = false,
  currentPage,
  pageSize,
  onPageChange,
  onPageSizeChange,
  emptyMessage = {
    title: 'No query results',
    description: 'Run a SQL query to see results here'
  }
}: PaginatedDataGridProps) {
  const columns = useMemo((): readonly Column<any>[] => {
    let columns: Column<any>[] = [];

    if (data && data.length > 0) {
      columns = data[0].map((header, index) => ({
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
  }, [data]);

  const rows = useMemo((): readonly Row[] => {
    let rows: Row[] = [];

    if (data && data.length > 1) {
      for (let i = 1; i < data.length; i++) {
        rows.push({
          id: String(i),
          title: data[i]
        });
      }
    }

    return rows;
  }, [data]);

  const handlePreviousPage = () => {
    if (currentPage > 0) {
      onPageChange(currentPage - 1);
    }
  };

  const handleNextPage = () => {
    if (paginationInfo?.has_next_page) {
      onPageChange(currentPage + 1);
    }
  };

  const handlePageJump = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const formData = new FormData(e.currentTarget);
    const pageInput = formData.get('page') as string;
    const targetPage = parseInt(pageInput, 10);

    if (!isNaN(targetPage) && targetPage >= 1) {
      const maxPage = paginationInfo?.total_pages || Infinity;
      const clampedPage = Math.min(targetPage, maxPage) - 1; // Convert to 0-based
      if (clampedPage !== currentPage) {
        onPageChange(Math.max(0, clampedPage));
      }
    }
  };

  return (
    <div className="flex-1 bg-white overflow-hidden relative flex flex-col">
      {data && data.length > 0 ? (
        <>
          <div className="flex-1 overflow-hidden">
            <DataGrid
              columns={columns}
              rows={rows}
              rowHeight={35}
              className="fill-grid"
              direction="ltr"
              enableVirtualization={true}
              rowKeyGetter={(row) => row.id}
            />
          </div>

          {/* Pagination Controls */}
          {paginationInfo && (
            <div className="border-t border-gray-200 bg-white px-4 py-2 flex items-center justify-between">
              <div className="flex items-center space-x-4">
                {/* Page size selector */}
                <div className="flex items-center space-x-2">
                  <label className="text-xs text-gray-600">Rows:</label>
                  <select
                    value={pageSize}
                    onChange={(e) => onPageSizeChange(Number(e.target.value))}
                    className="text-xs border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  >
                    <option value={10}>10</option>
                    <option value={25}>25</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                    <option value={1000}>1000</option>
                    <option value={10000}>10000</option>
                  </select>
                </div>

                {/* Row count info */}
                <div className="text-xs text-gray-600">
                  Showing {paginationInfo.rows_in_page} row
                  {paginationInfo.rows_in_page !== 1 ? 's' : ''}
                  {paginationInfo.total_rows && (
                    <span>
                      {' '}
                      of {paginationInfo.total_rows.toLocaleString()}
                    </span>
                  )}
                </div>
              </div>

              {/* Navigation buttons */}
              <div className="flex items-center space-x-2">
                <button
                  onClick={handlePreviousPage}
                  disabled={!paginationInfo.has_previous_page || isLoading}
                  className="p-1.5 text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Previous page"
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
                      d="M15 19l-7-7 7-7"
                    />
                  </svg>
                </button>

                <form
                  onSubmit={handlePageJump}
                  className="flex items-center space-x-2"
                >
                  <span className="text-xs text-gray-600">Page</span>
                  <input
                    type="number"
                    name="page"
                    min="1"
                    max={paginationInfo.total_pages || undefined}
                    defaultValue={currentPage + 1}
                    key={currentPage} // Reset input when page changes externally
                    className="w-16 text-xs text-center border border-gray-300 rounded px-2 py-1 focus:outline-none focus:ring-1 focus:ring-blue-500"
                  />
                  {paginationInfo.total_pages && (
                    <span className="text-xs text-gray-600">
                      of {paginationInfo.total_pages}
                    </span>
                  )}
                  <button
                    type="submit"
                    className="text-xs px-2 py-1 text-blue-600 hover:text-blue-700 font-medium"
                  >
                    Go
                  </button>
                </form>

                <button
                  onClick={handleNextPage}
                  disabled={!paginationInfo.has_next_page || isLoading}
                  className="p-1.5 text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Next page"
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
                      d="M9 5l7 7-7 7"
                    />
                  </svg>
                </button>
              </div>
            </div>
          )}
        </>
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
                d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
              />
            </svg>
            <p className="text-lg font-medium text-gray-900 mb-1">
              {emptyMessage.title}
            </p>
            <p className="text-sm text-gray-500">{emptyMessage.description}</p>
          </div>
        </div>
      )}
    </div>
  );
}
