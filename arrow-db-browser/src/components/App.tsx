import 'react-data-grid/lib/styles.css';
import DataGrid, { Column, RenderCellProps } from 'react-data-grid';
import { useMemo, useState, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import './../assets/base.css';
//@ts-ignore
import init, { ArrowDbWasm } from './../../arrow-db-wasm';
import TableExplorer from './TableExplorer';
import FileUpload from './FileUpload';
import AlertModal from './AlertModal';
import DataView from './DataView';
import StructureView from './StructureView';

interface Cell {
  id: string;
  title: string[];
}
type Row = Cell;
let database: ArrowDbWasm;

// load the database once
let initPromise = init()
  .then(() => {
    console.log('WASM module loaded, initializing database');
    database = new ArrowDbWasm('test');
    console.log('Database initialized successfully:', database);
    return database;
  })
  .catch((error) => {
    console.error('Failed to initialize WASM/database:', error);
    throw error;
  });

export default function App() {
  const [output, setOutput] = useState<string[][] | null>(null);
  const [query, setQuery] = useState<string>('');
  const [schemas, setSchemas] = useState<any[] | null>(null);
  const [tables, setTables] = useState<string[]>([]);
  const [isDatabaseReady, setIsDatabaseReady] = useState<boolean>(false);
  const [isQueryLoading, setIsQueryLoading] = useState<boolean>(false);
  const [isFileLoading, setIsFileLoading] = useState<boolean>(false);
  const [loadingProgress, setLoadingProgress] = useState<{
    current: number;
    total: number;
    fileName?: string;
  } | null>(null);
  const [alert, setAlert] = useState<{
    isOpen: boolean;
    title: string;
    message: string;
    type: 'danger' | 'warning' | 'info' | 'success';
  }>({ isOpen: false, title: '', message: '', type: 'info' });
  const [viewMode, setViewMode] = useState<'sql' | 'data' | 'structure'>('sql');
  const [selectedTableForData, setSelectedTableForData] = useState<
    string | null
  >(null);
  const [selectedTableForStructure, setSelectedTableForStructure] = useState<
    string | null
  >(null);

  // Helper functions for alerts
  const showAlert = (
    title: string,
    message: string,
    type: 'danger' | 'warning' | 'info' | 'success' = 'info'
  ) => {
    setAlert({ isOpen: true, title, message, type });
  };

  const hideAlert = useCallback(() => {
    setAlert({ isOpen: false, title: '', message: '', type: 'info' });
  }, []);

  useEffect(() => {
    initPromise
      .then(() => {
        setIsDatabaseReady(true);
      })
      .catch((error) => {
        console.error('Database initialization failed:', error);
        setIsDatabaseReady(false);
      });
  }, []);

  const handleQuery = () => {
    if (query !== '') {
      console.log('Executing query:', query);
      console.log('Database ready:', isDatabaseReady);
      console.log('Database instance:', database);

      if (!isDatabaseReady || !database) {
        console.error('Database not ready yet');
        showAlert(
          'Database Not Ready',
          'Database not ready. Please wait a moment and try again.',
          'warning'
        );
        return;
      }

      // Clear previous results and show loading
      setOutput(null);
      setIsQueryLoading(true);

      // Use MessageChannel to defer execution and allow React to render loading state
      const channel = new MessageChannel();
      channel.port2.onmessage = () => {
        database
          .query(query)
          .then((results) => {
            if (results && results[0] && results[0].data) {
              setOutput(results[0].data);
            } else {
              console.error('Unexpected query result format:', results);
              showAlert(
                'Query Error',
                'Query executed but returned unexpected format',
                'warning'
              );
            }
          })
          .catch((error) => {
            console.error('Query error:', error);
            showAlert(
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
      showAlert('No Query', 'Please enter a query', 'info');
    }
  };

  const handleTableSelect = (tableName: string) => {
    // Optional: Could highlight the selected table or show schema info
    console.log(`Selected table: ${tableName}`);
  };

  const handleTableDoubleClick = (tableName: string) => {
    console.log('Double-clicked table:', tableName);
    setQuery(`SELECT * FROM ${tableName}`);
  };

  const handleQueryTableData = async (
    tableName: string
  ): Promise<string[][]> => {
    if (!isDatabaseReady || !database) {
      throw new Error('Database not ready');
    }

    try {
      const results = await database.query(`SELECT * FROM ${tableName}`);
      if (results && results[0] && results[0].data) {
        return results[0].data;
      }
      return [];
    } catch (error) {
      console.error('Error querying table:', error);
      throw error;
    }
  };

  const handleTableRemove = (tableName: string) => {
    if (!isDatabaseReady || !database) {
      console.error('Database not ready yet');
      showAlert(
        'Database Not Ready',
        'Database not ready. Please wait a moment and try again.',
        'warning'
      );
      return;
    }

    try {
      // First check if the table actually exists
      const currentTables = database.get_tables();
      console.log('Current tables before removal:', currentTables);

      if (!currentTables.includes(tableName)) {
        console.warn(
          `Table ${tableName} not found in database, updating UI state`
        );
        // Table doesn't exist in database, just update UI state
        const filteredTables = tables.filter((t) => t !== tableName);
        setTables(filteredTables);

        // Update schemas as well
        if (schemas) {
          const filteredSchemas = schemas.filter(
            (s) => s.table_name !== tableName
          );
          setSchemas(filteredSchemas);
        }

        // Clear output if the removed table was being queried
        setOutput(null);

        return;
      }

      // Table exists, proceed with removal
      database.remove_table(tableName);

      // Update the tables and schemas state
      const updatedTables = database.get_tables();
      const updatedSchemas = database.get_schemas();

      setTables(updatedTables);
      setSchemas(updatedSchemas);

      // Clear output if the removed table was being queried
      setOutput(null);

      console.log(`Table ${tableName} removed successfully`);
    } catch (error) {
      console.error('Error removing table:', error);
      showAlert('Remove Failed', `Failed to remove table: ${error}`, 'danger');
    }
  };

  const handleFileSelect = async (files: File[]) => {
    if (files.length === 0) return;

    setIsFileLoading(true);
    setLoadingProgress({ current: 0, total: files.length });

    try {
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        const tableName = file.name.substring(0, file.name.lastIndexOf('.'));

        setLoadingProgress({
          current: i + 1,
          total: files.length,
          fileName: file.name
        });

        // Read file as ArrayBuffer
        const arrayBuffer = await new Promise<ArrayBuffer>(
          (resolve, reject) => {
            const reader = new FileReader();
            reader.onload = (e) => {
              if (e.target?.result) {
                resolve(e.target.result as ArrayBuffer);
              } else {
                reject(new Error('Failed to read file'));
              }
            };
            reader.onerror = () => reject(new Error('Failed to read file'));
            reader.readAsArrayBuffer(file);
          }
        );

        const bytes = new Uint8Array(arrayBuffer);

        // Use MessageChannel to defer WASM execution and allow React to render loading state
        await new Promise<void>((resolve, reject) => {
          const channel = new MessageChannel();
          channel.port2.onmessage = () => {
            try {
              database.read_file(tableName, bytes);
              resolve();
            } catch (error) {
              console.error(`Error loading file ${file.name}:`, error);
              reject(new Error(`Failed to load file ${file.name}: ${error}`));
            }
          };
          channel.port1.postMessage(null);
        });
      }

      // Update schemas and tables after all files are loaded
      const schemas = database.get_schemas();
      const tables = database.get_tables();
      setSchemas(schemas);
      setTables(tables);
    } catch (error) {
      console.error('Error loading files:', error);
      showAlert('Upload Failed', `Failed to load files: ${error}`, 'danger');
    } finally {
      setIsFileLoading(false);
      setLoadingProgress(null);
    }
  };

  const columns = useMemo((): readonly Column<any>[] => {
    let columns: Column<any>[] = [];

    if (output) {
      columns = output[0].map((header, index) => ({
        key: String(index),
        name: header,
        width: 120,
        minWidth: 80,
        resizable: true,
        renderCell: (props: RenderCellProps<Cell>) =>
          `${props.row.title[index]}`
      }));
    }

    return columns;
  }, [output]);

  const rows = useMemo((): readonly Row[] => {
    let rows: Row[] = [];

    if (output) {
      // Remove the arbitrary row limit - let the grid handle virtualization
      for (let i = 1; i < output.length; i++) {
        rows.push({
          id: String(i),
          title: output[i]
        });
      }
    }

    return rows;
  }, [output]);

  return (
    <div className="h-screen flex flex-col bg-gray-100">
      {/* Top Toolbar */}
      <div className="bg-white border-b border-gray-200 px-4 py-3 flex items-center justify-between">
        <div className="flex items-center space-x-4">
          <h1 className="text-lg font-semibold text-gray-900">
            Arrow DB Browser
          </h1>
          {tables.length > 0 && (
            <div className="text-sm text-gray-600 flex items-center space-x-2">
              <svg
                className="w-4 h-4 text-green-500"
                fill="currentColor"
                viewBox="0 0 20 20"
              >
                <path
                  fillRule="evenodd"
                  d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                  clipRule="evenodd"
                />
              </svg>
              <span>
                {tables.length} table{tables.length !== 1 ? 's' : ''} loaded
              </span>
            </div>
          )}
        </div>
        {tables.length > 0 && (
          <button
            onClick={() => {
              setTables([]);
              setSchemas(null);
              setOutput(null);
              setQuery('');
              setLoadingProgress(null);
            }}
            className="text-sm text-gray-500 hover:text-gray-700 px-3 py-1 rounded-md hover:bg-gray-100"
          >
            Load New File
          </button>
        )}
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {tables.length === 0 ? (
          /* File Upload Screen */
          <div className="flex-1 flex items-center justify-center bg-gray-50">
            <FileUpload
              onFileSelect={handleFileSelect}
              isLoading={isFileLoading}
              disabled={!isDatabaseReady}
              loadingProgress={loadingProgress || undefined}
              onShowAlert={showAlert}
            />
          </div>
        ) : (
          /* Database Explorer Interface */
          <>
            {/* Left Sidebar - Table Explorer */}
            <div className="w-64 flex-shrink-0">
              <TableExplorer
                tables={tables}
                schemas={schemas}
                onTableSelect={handleTableSelect}
                onTableDoubleClick={handleTableDoubleClick}
                onTableRemove={handleTableRemove}
                onTableClickForData={(tableName) => {
                  setViewMode('data');
                  setSelectedTableForData(tableName);
                }}
                onTableClickForStructure={(tableName) => {
                  setViewMode('structure');
                  setSelectedTableForStructure(tableName);
                }}
                selectedTableForData={selectedTableForData}
                selectedTableForStructure={selectedTableForStructure}
                viewMode={viewMode}
              />
            </div>

            {/* Main Content */}
            <div className="flex-1 flex flex-col">
              {/* Tab Navigation */}
              <div className="bg-white border-b border-gray-200">
                <div className="flex space-x-1 px-4 pt-3">
                  <button
                    onClick={() => setViewMode('sql')}
                    className={`px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${
                      viewMode === 'sql'
                        ? 'bg-white text-blue-600 border-t border-l border-r border-gray-200'
                        : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                    }`}
                  >
                    <svg
                      className="w-4 h-4 inline-block mr-2"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4"
                      />
                    </svg>
                    SQL Query
                  </button>
                  <button
                    onClick={() => setViewMode('data')}
                    className={`px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${
                      viewMode === 'data'
                        ? 'bg-white text-blue-600 border-t border-l border-r border-gray-200'
                        : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                    }`}
                  >
                    <svg
                      className="w-4 h-4 inline-block mr-2"
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
                    Data View
                  </button>
                  <button
                    onClick={() => setViewMode('structure')}
                    className={`px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${
                      viewMode === 'structure'
                        ? 'bg-white text-purple-600 border-t border-l border-r border-gray-200'
                        : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                    }`}
                  >
                    <svg
                      className="w-4 h-4 inline-block mr-2"
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
                    Structure
                  </button>
                </div>
              </div>

              {/* SQL Query Panel */}
              {viewMode === 'sql' && (
                <>
                  <div className="bg-white border-b border-gray-200 p-4">
                    <div className="flex items-center justify-between mb-2">
                      <label className="text-sm font-medium text-gray-700">
                        SQL Query
                      </label>
                      <button
                        className={`px-4 py-2 text-white text-sm font-medium rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 flex items-center space-x-2 ${
                          isDatabaseReady && !isQueryLoading
                            ? 'bg-blue-600 hover:bg-blue-700'
                            : 'bg-gray-400 cursor-not-allowed'
                        }`}
                        onClick={handleQuery}
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
                    <textarea
                      className="w-full h-24 px-3 py-2 border border-gray-300 rounded-md text-sm font-mono resize-none focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                      value={query}
                      onChange={(e) => setQuery(e.target.value || '')}
                      onKeyDown={(e) => {
                        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
                          e.preventDefault();
                          handleQuery();
                        }
                      }}
                      placeholder="SELECT * FROM table_name (Ctrl+Enter to run)"
                      disabled={isQueryLoading}
                    />
                  </div>

                  {/* Results Grid */}
                  <div className="flex-1 bg-white overflow-hidden relative">
                    {/* Grid Content */}
                    {output ? (
                      <DataGrid
                        columns={columns}
                        rows={rows}
                        rowHeight={32}
                        className="fill-grid"
                        direction="ltr"
                        enableVirtualization={true}
                        rowKeyGetter={(row) => row.id}
                      />
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
                            No query results
                          </p>
                          <p className="text-sm text-gray-500">
                            Run a SQL query to see results here
                          </p>
                        </div>
                      </div>
                    )}
                  </div>
                </>
              )}

              {/* Data View */}
              {viewMode === 'data' && (
                <DataView
                  tables={tables}
                  selectedTable={selectedTableForData}
                  onTableSelect={setSelectedTableForData}
                  onQueryTable={handleQueryTableData}
                  isDatabaseReady={isDatabaseReady}
                />
              )}

              {/* Structure View */}
              {viewMode === 'structure' && (
                <StructureView
                  selectedTable={selectedTableForStructure}
                  schemas={schemas}
                />
              )}
            </div>
          </>
        )}
      </div>

      {/* Portal-based Loading Overlay */}
      {(isQueryLoading || isFileLoading) &&
        createPortal(
          <div
            style={{
              position: 'fixed',
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              backgroundColor: 'rgba(255, 255, 255, 0.2)',
              zIndex: 999999,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center'
            }}
          >
            <div style={{ textAlign: 'center', color: '#374151' }}>
              <div
                style={{
                  width: '48px',
                  height: '48px',
                  border: '4px solid rgba(59, 130, 246, 0.3)',
                  borderTop: '4px solid #3b82f6',
                  borderRadius: '50%',
                  animation: 'spin 1s linear infinite',
                  margin: '0 auto 16px'
                }}
              ></div>
              <h3
                style={{
                  fontSize: '18px',
                  fontWeight: 'bold',
                  margin: '0 0 8px'
                }}
              >
                {isFileLoading ? 'Loading File...' : 'Running Query...'}
              </h3>
              <p style={{ fontSize: '14px', margin: 0, opacity: 0.8 }}>
                {isFileLoading
                  ? 'Please wait while we process your file'
                  : 'Please wait while we process your query'}
              </p>
            </div>
          </div>,
          document.body
        )}

      {/* Alert Modal */}
      <AlertModal
        isOpen={alert.isOpen}
        title={alert.title}
        message={alert.message}
        onClose={hideAlert}
        type={alert.type}
      />
    </div>
  );
}
