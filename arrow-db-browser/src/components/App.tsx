import 'react-data-grid/lib/styles.css';
import DataGrid, { Column, RenderCellProps } from 'react-data-grid';
import { useMemo, useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import './../assets/base.css';
//@ts-ignore
import init, { ArrowDbWasm } from './../../arrow-db-wasm';
import TableExplorer from './TableExplorer';

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
        alert('Database not ready. Please wait a moment and try again.');
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
              alert('Query executed but returned unexpected format');
            }
          })
          .catch((error) => {
            console.error('Query error:', error);
            alert(`Query failed: ${error.message || error}`);
          })
          .finally(() => {
            setIsQueryLoading(false);
          });
      };
      channel.port1.postMessage(null);
    } else {
      alert('Please enter a query');
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

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];

    if (file) {
      setIsFileLoading(true);
      const reader = new FileReader();

      reader.onload = (e) => {
        if (e.target) {
          const bytes = new Uint8Array(e.target.result as ArrayBuffer);
          const tableName = file.name.substring(0, file.name.lastIndexOf('.'));

          // Use MessageChannel to defer WASM execution and allow React to render loading state
          const channel = new MessageChannel();
          channel.port2.onmessage = () => {
            try {
              database.read_file(tableName, bytes);
              const schemas = database.get_schemas();
              const tables = database.get_tables();
              setSchemas(schemas);
              setTables(tables);
            } catch (error) {
              console.error('Error loading file:', error);
              alert(`Failed to load file: ${error}`);
            } finally {
              setIsFileLoading(false);
            }
          };
          channel.port1.postMessage(null);
        }
      };

      reader.onerror = () => {
        setIsFileLoading(false);
        alert('Failed to read file');
      };

      reader.readAsArrayBuffer(file);
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
          <input
            type="file"
            onChange={handleFileChange}
            disabled={isFileLoading}
            className={`text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-medium ${
              isFileLoading
                ? 'file:bg-gray-100 file:text-gray-400 cursor-not-allowed'
                : 'file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100'
            }`}
          />
          {isFileLoading && (
            <span className="ml-2 text-sm text-gray-600 flex items-center">
              <svg
                className="animate-spin -ml-1 mr-2 h-4 w-4 text-blue-500"
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
              Loading file...
            </span>
          )}
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Sidebar - Table Explorer */}
        <div className="w-64 flex-shrink-0">
          <TableExplorer
            tables={tables}
            schemas={schemas}
            onTableSelect={handleTableSelect}
            onTableDoubleClick={handleTableDoubleClick}
          />
        </div>

        {/* Main Content */}
        <div className="flex-1 flex flex-col">
          {/* SQL Query Panel */}
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
              <></>
            )}
          </div>
        </div>
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
    </div>
  );
}
