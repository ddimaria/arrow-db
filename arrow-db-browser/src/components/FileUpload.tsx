import React, { useState } from 'react';

function FileUpload() {
  const [fileBytes, setFileBytes] = useState<ArrayBuffer | null>(null);

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];

    if (file) {
      const reader = new FileReader();

      reader.onload = (e) => {
        if (e.target) {
          setFileBytes(e.target.result as ArrayBuffer);
        }
      };

      reader.readAsArrayBuffer(file); // Read the file as an ArrayBuffer
    }
  };

  return (
    <div>
      <input type="file" onChange={handleFileChange} />
      {fileBytes && (
        <div>
          <h2>File Bytes:</h2>
          <pre>
            {JSON.stringify(Array.from(new Uint8Array(fileBytes)), null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}

export default FileUpload;
