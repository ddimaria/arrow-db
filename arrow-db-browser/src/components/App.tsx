import 'react-data-grid/lib/styles.css';
import DataGrid, { Column, RenderCellProps } from 'react-data-grid';
import { useMemo, useState } from 'react';
import './../assets/base.css';
//@ts-ignore
import init, { ArrowDbWasm } from './../../arrow-db-wasm';

interface Cell {
  id: string;
  title: string[];
}
type Row = Cell;
let database: ArrowDbWasm;

// load the database once
init().then(() => {
  console.log('Loading database');
  database = new ArrowDbWasm('test');
});

export default function App() {
  const [output, setOutput] = useState<string[][] | null>(null);
  const [query, setQuery] = useState<string>('');
  const [schemas, setSchemas] = useState<string[] | null>(null);

  const handleQuery = () => {
    if (query !== '') {
      database.query(query).then((results) => {
        setOutput(results[0].data);
      });
    }
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];

    if (file) {
      const reader = new FileReader();

      reader.onload = (e) => {
        if (e.target) {
          const bytes = new Uint8Array(e.target.result as ArrayBuffer);
          const tableName = file.name.substring(0, file.name.lastIndexOf('.'));
          database.read_file(tableName, bytes).then(() => {
            setSchemas(database.get_schemas());
            console.log(`Loaded table ${tableName}`);
          });
        }
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
        // width: 80,
        resizable: true,
        frozen: true,
        renderCell: (props: RenderCellProps<Cell>) =>
          `${props.row.title[index]}`
      }));
    }

    return columns;
  }, [output]);

  const rows = useMemo((): readonly Row[] => {
    let rows: Row[] = [];

    let maxRows = 1000;

    if (output) {
      for (let i = 1; i < Math.min(maxRows, output.length); i++) {
        rows.push({
          id: String(i),
          title: output[i]
        });
      }
    }

    return rows;
  }, [output]);

  return (
    <div className="h-full">
      <div>
        <input type="file" onChange={handleFileChange} />
        <div className="grid grid-flow-row-dense grid-cols-8">
          <div className="col-span-6">
            <DataGrid
              columns={columns}
              rows={rows}
              rowHeight={22}
              className="fill-grid"
              direction="ltr"
            />
          </div>
          <div className="col-span-2 h-full pl-10 pr-10">
            <div className="fixed">
              <div className="text-sm text-gray-500">SQL</div>
              <textarea
                className="w-full border-2 border-gray-300 h-100"
                value={query}
                onChange={(e) => setQuery(e.target.value || '')}
              />
              <button
                className="bg-blue-500 text-white p-2 rounded-md"
                onClick={handleQuery}
              >
                Query
              </button>
              <div>
                {schemas && (
                  <div className="text-md text-black-800 mt-5">
                    Loaded Tables
                  </div>
                )}
                {schemas &&
                  schemas.map((schema, index) => (
                    <div className="text-sm text-black-500 mt-2" key={index}>
                      {schema}
                    </div>
                  ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
