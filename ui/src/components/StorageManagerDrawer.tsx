import {
  Drawer, Box, Typography, IconButton, Button,
  Table, TableBody, TableCell, TableHead, TableRow,
  TextField, Breadcrumbs, Link, Alert
} from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import DeleteIcon from '@mui/icons-material/Delete';
import CreateNewFolderIcon from '@mui/icons-material/CreateNewFolder';
import InsertDriveFileIcon from '@mui/icons-material/InsertDriveFile';
import { useState, useEffect, useCallback } from 'react';
import { useProjectContext } from '../contexts/ProjectContext';
import { checkedMutation, safeRequestError } from '../apiClient';

type StorageManagerDrawerProps = {
  open: boolean;
  onClose: () => void;
};

type StorageBucket = {
  name: string;
};

type StorageObject = {
  name: string;
  size?: string;
};

export default function StorageManagerDrawer({ open, onClose }: StorageManagerDrawerProps) {
  const { activeProject } = useProjectContext();
  const [buckets, setBuckets] = useState<StorageBucket[]>([]);
  const [newBucketName, setNewBucketName] = useState('');
  
  const [currentBucket, setCurrentBucket] = useState<string | null>(null);
  const [objects, setObjects] = useState<StorageObject[]>([]);
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadBuckets = useCallback(async () => {
    try {
      const res = await fetch(`/api/manage/storage/b?project=${activeProject}`);
      if (res.ok) {
        const data = await res.json() as { items?: StorageBucket[] };
        setBuckets(data.items || []);
      }
    } catch (e) {
      console.error(e);
    }
  }, [activeProject]);

  const loadObjects = async (bucket: string) => {
    try {
      const res = await fetch(`/api/manage/storage/b/${bucket}/o`);
      if (res.ok) {
        const data = await res.json() as { items?: StorageObject[] };
        setObjects(data.items || []);
      }
    } catch (e) {
      console.error(e);
    }
  };

  useEffect(() => {
    if (open) {
      loadBuckets();
      setCurrentBucket(null);
    }
  }, [open, loadBuckets]);

  useEffect(() => {
    if (currentBucket) {
      loadObjects(currentBucket);
    }
  }, [currentBucket]);

  const handleCreateBucket = async () => {
    if (!newBucketName) return;
    try {
      await checkedMutation(`/api/manage/storage/b?project=${activeProject}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newBucketName }),
      }, 'Bucket creation failed. Check the bucket name and retry.');
      setNewBucketName('');
      loadBuckets();
    } catch (cause) {
      setError(safeRequestError(cause, 'Unable to connect while creating the bucket.'));
    }
  };

  const handleDeleteBucket = async (name: string) => {
    if (!confirm(`Delete bucket "${name}" and all contents?`)) return;
    try {
      await checkedMutation(`/api/manage/storage/b/${name}`, { method: 'DELETE' },
        'Bucket deletion failed. Remove all objects before retrying.');
      loadBuckets();
    } catch (cause) {
      setError(safeRequestError(cause, 'Unable to connect while deleting the bucket.'));
    }
  };

  const handleUpload = async () => {
    if (!uploadFile || !currentBucket) return;
    const formData = new FormData();
    formData.append('file', uploadFile);
    try {
      await checkedMutation(`/api/manage/storage/b/${currentBucket}/o?name=${encodeURIComponent(uploadFile.name)}`, {
        method: 'POST',
        body: formData,
      }, 'Object upload failed. Check the file and bucket, then retry.');
      setUploadFile(null);
      loadObjects(currentBucket);
    } catch (cause) {
      setError(safeRequestError(cause, 'Unable to connect while uploading the object.'));
    }
  };

  const handleDeleteObject = async (name: string) => {
    if (!currentBucket) return;
    try {
      await checkedMutation(`/api/manage/storage/b/${currentBucket}/o/${encodeURIComponent(name)}`, { method: 'DELETE' },
        'Object deletion failed. Refresh the bucket and retry.');
      loadObjects(currentBucket);
    } catch (cause) {
      setError(safeRequestError(cause, 'Unable to connect while deleting the object.'));
    }
  };

  return (
    <Drawer anchor="right" open={open} onClose={onClose}>
      <Box sx={{ width: '600px', p: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Typography variant="h5" sx={{ fontWeight: 500 }}>Cloud Storage Manager</Typography>
        <IconButton aria-label="Close" onClick={onClose}><CloseIcon /></IconButton>
      </Box>
      {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}

      {currentBucket ? (
        <Box>
          <Breadcrumbs sx={{ mb: 3 }}>
            <Link component="button" variant="body1" onClick={() => setCurrentBucket(null)}>Buckets</Link>
            <Typography variant="body1" color="text.primary">{currentBucket}</Typography>
          </Breadcrumbs>
          
          <Box sx={{ display: 'flex', gap: 2, mb: 3, alignItems: 'center' }}>
            <input type="file" onChange={e => setUploadFile(e.target.files?.[0] || null)} />
            <Button variant="contained" onClick={handleUpload} disabled={!uploadFile}>Upload Object</Button>
          </Box>

          <Table size="small">
            <TableHead><TableRow><TableCell>Name</TableCell><TableCell>Size</TableCell><TableCell>Actions</TableCell></TableRow></TableHead>
            <TableBody>
              {objects.length === 0 && <TableRow><TableCell colSpan={3} align="center">No objects found</TableCell></TableRow>}
              {objects.map(o => (
                <TableRow key={o.name}>
                  <TableCell><Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}><InsertDriveFileIcon fontSize="small"/> {o.name}</Box></TableCell>
                  <TableCell>{Math.round(parseInt(o.size || '0') / 1024)} KB</TableCell>
                  <TableCell>
                    <IconButton aria-label={`Delete object ${o.name}`} size="small" color="error" onClick={() => handleDeleteObject(o.name)}><DeleteIcon fontSize="small"/></IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>
      ) : (
        <Box>
          <Box sx={{ display: 'flex', gap: 2, mb: 3 }}>
            <TextField size="small" label="New Bucket Name" value={newBucketName} onChange={e => setNewBucketName(e.target.value)} fullWidth />
            <Button variant="contained" onClick={handleCreateBucket}>Create</Button>
          </Box>

          <Table size="small">
            <TableHead><TableRow><TableCell>Bucket Name</TableCell><TableCell align="right">Actions</TableCell></TableRow></TableHead>
            <TableBody>
              {buckets.length === 0 && <TableRow><TableCell colSpan={2} align="center">No buckets found</TableCell></TableRow>}
              {buckets.map(b => (
                <TableRow key={b.name} hover sx={{ cursor: 'pointer' }} onClick={() => setCurrentBucket(b.name)}>
                  <TableCell><Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}><CreateNewFolderIcon fontSize="small"/> {b.name}</Box></TableCell>
                  <TableCell align="right">
                    <IconButton aria-label={`Delete bucket ${b.name}`} size="small" color="error" onClick={(e) => { e.stopPropagation(); handleDeleteBucket(b.name); }}><DeleteIcon fontSize="small"/></IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>
      )}
      </Box>
    </Drawer>
  );
}
