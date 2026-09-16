// Direct-to-MinIO upload flow:
// 1. POST /files/upload-url { name, size, folder_id } -> { upload_url, storage_key }
// 2. PUT file bytes to upload_url (progress via XHR upload.onprogress)
// 3. POST /files/upload-complete { storage_key, folder_id, name, size, mime_type }
async function uploadFile(file, folderId) {
  const res = await fetch("/files/upload-url", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name: file.name, size: file.size, folder_id: folderId }),
  });
  const { upload_url, storage_key } = await res.json();

  await fetch(upload_url, { method: "PUT", body: file });

  await fetch("/files/upload-complete", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      storage_key, folder_id: folderId, name: file.name,
      size: file.size, mime_type: file.type,
    }),
  });

  document.body.dispatchEvent(new Event("refreshFileList"));
}
