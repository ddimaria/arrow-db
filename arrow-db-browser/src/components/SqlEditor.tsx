import React, { useRef } from 'react';
import Editor from '@monaco-editor/react';

interface SchemaField {
  name: string;
  data_type: string;
  nullable: boolean;
}

interface TableSchema {
  table_name: string;
  fields: SchemaField[];
}

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  schemas: TableSchema[] | null;
  isLoading: boolean;
  isDatabaseReady: boolean;
  onRunQuery: () => void;
  height?: string;
}

export default function SqlEditor({
  value,
  onChange,
  schemas,
  isLoading,
  isDatabaseReady,
  onRunQuery,
  height = '150px'
}: SqlEditorProps) {
  const editorRef = useRef<any>(null);

  return (
    <div className="border border-gray-300 rounded-md overflow-hidden">
      <div className="pl-3">
        <Editor
          height={height}
          defaultLanguage="sql"
          value={value}
          onChange={(value) => onChange(value || '')}
          onMount={(editor, monaco) => {
            editorRef.current = editor;
            // Add Ctrl+Enter / Cmd+Enter shortcut to run query
            editor.addCommand(
              (window.navigator.platform.match('Mac') ? 2048 : 2056) | 3, // Ctrl+Enter or Cmd+Enter
              () => {
                if (isDatabaseReady && !isLoading) {
                  onRunQuery();
                }
              }
            );

            // Register SQL completions with schema awareness
            monaco.languages.registerCompletionItemProvider('sql', {
              provideCompletionItems: (model, position) => {
                const keywords = [
                  'SELECT',
                  'FROM',
                  'WHERE',
                  'JOIN',
                  'LEFT',
                  'RIGHT',
                  'INNER',
                  'OUTER',
                  'ON',
                  'AND',
                  'OR',
                  'NOT',
                  'IN',
                  'LIKE',
                  'BETWEEN',
                  'IS',
                  'NULL',
                  'ORDER',
                  'BY',
                  'ASC',
                  'DESC',
                  'GROUP',
                  'HAVING',
                  'LIMIT',
                  'OFFSET',
                  'INSERT',
                  'INTO',
                  'VALUES',
                  'UPDATE',
                  'SET',
                  'DELETE',
                  'CREATE',
                  'TABLE',
                  'DROP',
                  'ALTER',
                  'ADD',
                  'COLUMN',
                  'AS',
                  'DISTINCT',
                  'COUNT',
                  'SUM',
                  'AVG',
                  'MIN',
                  'MAX',
                  'CASE',
                  'WHEN',
                  'THEN',
                  'ELSE',
                  'END'
                ];

                const word = model.getWordUntilPosition(position);
                const range = {
                  startLineNumber: position.lineNumber,
                  endLineNumber: position.lineNumber,
                  startColumn: word.startColumn,
                  endColumn: word.endColumn
                };

                const suggestions: any[] = [];

                // Add SQL keywords
                suggestions.push(
                  ...keywords.map((keyword) => ({
                    label: keyword,
                    kind: monaco.languages.CompletionItemKind.Keyword,
                    insertText: keyword,
                    detail: 'SQL Keyword',
                    range: range,
                    sortText: '0' + keyword // Sort keywords first
                  }))
                );

                // Add table names from schema
                if (schemas) {
                  schemas.forEach((tableSchema) => {
                    suggestions.push({
                      label: tableSchema.table_name,
                      kind: monaco.languages.CompletionItemKind.Class,
                      insertText: `"${tableSchema.table_name}"`,
                      detail: `Table (${tableSchema.fields.length} columns)`,
                      documentation: tableSchema.fields
                        .map((f) => `${f.name}: ${f.data_type}`)
                        .join('\n'),
                      range: range,
                      sortText: '1' + tableSchema.table_name
                    });

                    // Add column names for this table
                    tableSchema.fields.forEach((field) => {
                      suggestions.push({
                        label: field.name,
                        kind: monaco.languages.CompletionItemKind.Field,
                        insertText: field.name,
                        detail: `${tableSchema.table_name}.${field.name} (${field.data_type})`,
                        documentation: `Column from ${tableSchema.table_name}\nType: ${field.data_type}\nNullable: ${field.nullable}`,
                        range: range,
                        sortText: '2' + field.name
                      });
                    });
                  });
                }

                return { suggestions };
              }
            });
          }}
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            lineNumbers: 'off',
            scrollBeyondLastLine: false,
            automaticLayout: true,
            tabSize: 2,
            readOnly: isLoading,
            wordWrap: 'on',
            wrappingIndent: 'same',
            padding: { top: 8, bottom: 8 },
            suggestOnTriggerCharacters: true,
            quickSuggestions: {
              other: true,
              comments: false,
              strings: false
            },
            suggest: {
              showKeywords: true,
              showSnippets: true
            },
            renderLineHighlight: 'none',
            contextmenu: true,
            folding: false,
            glyphMargin: false,
            lineDecorationsWidth: 0
          }}
          loading={
            <div
              className="w-full flex items-center justify-center bg-gray-50"
              style={{ height }}
            >
              <span className="text-sm text-gray-500">Loading editor...</span>
            </div>
          }
        />
      </div>
    </div>
  );
}
