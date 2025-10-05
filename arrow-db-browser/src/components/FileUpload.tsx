import React, { useState, useRef, DragEvent } from 'react';

interface FileUploadProps {
  onFileSelect: (files: File[]) => void;
  isLoading?: boolean;
  disabled?: boolean;
  loadingProgress?: { current: number; total: number; fileName?: string };
  onShowAlert?: (
    title: string,
    message: string,
    type?: 'danger' | 'warning' | 'info' | 'success'
  ) => void;
}

function FileUpload({
  onFileSelect,
  isLoading = false,
  disabled = false,
  loadingProgress,
  onShowAlert
}: FileUploadProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const validateFiles = (files: File[]): File[] => {
    const validFiles: File[] = [];
    const invalidFiles: string[] = [];

    files.forEach((file) => {
      const isParquet = file.name.toLowerCase().endsWith('.parquet');
      if (isParquet) {
        validFiles.push(file);
      } else {
        invalidFiles.push(file.name);
      }
    });

    if (invalidFiles.length > 0) {
      const message = `The following files are not valid Parquet files and will be skipped:\n${invalidFiles.join(
        '\n'
      )}\n\nOnly .parquet files are supported.`;

      if (onShowAlert) {
        onShowAlert('Invalid Files', message, 'warning');
      } else {
        // Fallback to browser alert if callback not provided
        alert(message);
      }
    }

    return validFiles;
  };

  const handleFileSelect = (files: File[]) => {
    const validFiles = validateFiles(files);
    if (validFiles.length > 0) {
      onFileSelect(validFiles);
    }
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (files && files.length > 0) {
      handleFileSelect(Array.from(files));
    }
    // Reset input value to allow selecting the same files again
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const handleDragOver = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    if (!disabled && !isLoading) {
      setIsDragOver(true);
    }
  };

  const handleDragLeave = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);
  };

  const handleDrop = (e: DragEvent<HTMLDivElement>) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragOver(false);

    if (disabled || isLoading) return;

    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      handleFileSelect(files);
    }
  };

  const handleClick = () => {
    if (!disabled && !isLoading && fileInputRef.current) {
      fileInputRef.current.click();
    }
  };

  return (
    <div className="w-full max-w-2xl mx-auto p-6">
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-gray-900 mb-2">
          Import Parquet Files
        </h2>
        <p className="text-sm text-gray-600">
          Upload one or more Parquet files to explore and query your data using
          SQL
        </p>
      </div>

      <div
        className={`
          relative border-2 border-dashed rounded-lg p-8 text-center cursor-pointer transition-all duration-200
          ${
            isDragOver && !disabled && !isLoading
              ? 'border-blue-400 bg-blue-50'
              : disabled || isLoading
                ? 'border-gray-200 bg-gray-50 cursor-not-allowed'
                : 'border-gray-300 hover:border-gray-400 hover:bg-gray-50'
          }
        `}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onDrop={handleDrop}
        onClick={handleClick}
      >
        <input
          ref={fileInputRef}
          type="file"
          accept=".parquet"
          multiple
          onChange={handleFileChange}
          disabled={disabled || isLoading}
          className="hidden"
        />

        <div className="space-y-4">
          {/* Upload Icon */}
          <div className="mx-auto w-12 h-12 flex items-center justify-center">
            {isLoading ? (
              <svg
                className="animate-spin h-8 w-8 text-blue-500"
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
            ) : (
              <svg
                className={`h-8 w-8 ${
                  disabled
                    ? 'text-gray-300'
                    : isDragOver
                      ? 'text-blue-500'
                      : 'text-gray-400'
                }`}
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
                />
              </svg>
            )}
          </div>

          {/* Upload Text */}
          <div>
            <p
              className={`text-lg font-medium ${
                disabled
                  ? 'text-gray-400'
                  : isDragOver
                    ? 'text-blue-600'
                    : 'text-gray-700'
              }`}
            >
              {isLoading
                ? loadingProgress
                  ? `Processing ${loadingProgress.fileName || 'file'}... (${
                      loadingProgress.current
                    }/${loadingProgress.total})`
                  : 'Processing files...'
                : isDragOver
                  ? 'Drop your Parquet files here'
                  : 'Drop your Parquet files here, or click to browse'}
            </p>
            {!isLoading && (
              <p className="text-sm text-gray-500 mt-1">
                Supports multiple .parquet files up to 100MB each
              </p>
            )}
          </div>

          {/* File Format Info */}
          {!isLoading && (
            <div className="flex items-center justify-center space-x-4 text-xs text-gray-400">
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-green-400 rounded-full"></div>
                <span>Parquet</span>
              </div>
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-blue-400 rounded-full"></div>
                <span>Columnar</span>
              </div>
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-purple-400 rounded-full"></div>
                <span>Compressed</span>
              </div>
            </div>
          )}
        </div>

        {/* Loading Progress Indicator */}
        {isLoading && (
          <div className="absolute inset-0 bg-white bg-opacity-75 flex items-center justify-center rounded-lg">
            <div className="text-center">
              <div className="text-sm font-medium text-gray-700 mb-2">
                {loadingProgress
                  ? `Loading ${loadingProgress.fileName || 'file'}... (${
                      loadingProgress.current
                    }/${loadingProgress.total})`
                  : 'Loading files...'}
              </div>
              <div className="w-32 h-2 bg-gray-200 rounded-full overflow-hidden">
                <div
                  className="h-full bg-blue-500 rounded-full transition-all duration-300"
                  style={{
                    width: loadingProgress
                      ? `${
                          (loadingProgress.current / loadingProgress.total) *
                          100
                        }%`
                      : '100%'
                  }}
                ></div>
              </div>
              {loadingProgress && (
                <div className="text-xs text-gray-500 mt-1">
                  {Math.round(
                    (loadingProgress.current / loadingProgress.total) * 100
                  )}
                  % complete
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Help Text */}
      <div className="mt-4 text-xs text-gray-500 space-y-1">
        <p>• Parquet files contain structured data in a columnar format</p>
        <p>
          • Upload multiple files to create multiple tables in your database
        </p>
        <p>• Once uploaded, you can query your data using SQL</p>
        <p>• Table names will be derived from your filenames</p>
      </div>
    </div>
  );
}

export default FileUpload;
