import React, { useState, useEffect } from 'react';
import {
  Card,
  CardContent,
  CardMedia,
  Typography,
  Box,
  Chip,
  IconButton,
  Divider,
  List,
  ListItem,
  ListItemText,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  CircularProgress,
} from '@mui/material';
import {
  Close as CloseIcon,
  OpenInNew as OpenIcon,
  Download as DownloadIcon,
  CalendarToday as DateIcon,
  Person as PersonIcon,
  Tag as TagIcon,
  Description as DescriptionIcon,
} from '@mui/icons-material';
import { QuiltRecord } from '../types/api';

interface RecordDetailProps {
  record: QuiltRecord;
  onClose: () => void;
}

interface Artifact {
  artifact_id: string;
  title: string;
  description?: string;
  image_urls?: string[];
  metadata?: Record<string, any>;
  [key: string]: any; // For any extra fields
}

const RecordDetail: React.FC<RecordDetailProps> = ({ record, onClose }) => {
  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
  };

  const formatDates = (dates?: string[]) => {
    if (!dates || dates.length === 0) return 'Not specified';
    return dates.join(', ');
  };

  const handleImageDownload = () => {
    if (record.image_url) {
      window.open(record.image_url, '_blank');
    }
  };

  // State for artifact popup
  const [selectedArtifact, setSelectedArtifact] = useState<Artifact | null>(null);

  // State for fetched artifacts
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [artifactsLoading, setArtifactsLoading] = useState<boolean>(false);

  // Fetch artifacts for this block_id when component mounts or record changes
  useEffect(() => {
    let isMounted = true;
    const fetchArtifacts = async () => {
      setArtifactsLoading(true);
      try {
        console.log('Fetching artifacts...', record.block_id);
        const resp = await fetch(`/api/artifacts?block_id=${encodeURIComponent(record.block_id)}`);
        const data = await resp.json();
        console.log('Fetched artifacts:', data);
        setArtifacts(data.artifacts || data || []);
      } catch (err) {
        if (isMounted) setArtifacts([]);
      } finally {
        if (isMounted) setArtifactsLoading(false);
      }
    };
    fetchArtifacts();
    return () => { isMounted = false; };
  }, [record.block_id]);

  return (
    <Card sx={{ height: 'fit-content', maxHeight: '80vh', overflow: 'auto' }}>
      <Box display="flex" justifyContent="space-between" alignItems="center" p={2} pb={0}>
        <Typography variant="h6" noWrap>
          Record Details
        </Typography>
        <IconButton onClick={onClose} size="small">
          <CloseIcon />
        </IconButton>
      </Box>

      <CardContent>
        {/* Image */}
        {record.image_url && (
          <Box mb={3}>
            <CardMedia
              component="img"
              image={record.image_url}
              alt={record.title}
              sx={{
                width: '100%',
                height: 200,
                objectFit: 'cover',
                borderRadius: 1,
                mb: 1,
              }}
            />
            <Box display="flex" gap={1}>
              <Button
                size="small"
                startIcon={<DownloadIcon />}
                onClick={handleImageDownload}
                variant="outlined"
              >
                View Full Size
              </Button>
            </Box>
          </Box>
        )}

        {/* Title */}
        <Typography variant="h6" mb={2} sx={{ wordBreak: 'break-word' }}>
          {record.title}
        </Typography>

        {/* Item ID */}
        <Box display="flex" alignItems="center" mb={2}>
          <Typography variant="body2" color="text.secondary">
            Item ID: {record.block_id}
          </Typography>
        </Box>

        <Divider sx={{ my: 2 }} />

        {/* Description */}
        {record.description && (
          <Box mb={3}>
            <Box display="flex" alignItems="center" mb={1}>
              <DescriptionIcon sx={{ mr: 1, color: 'text.secondary' }} />
              <Typography variant="subtitle2" fontWeight="bold">
                Description
              </Typography>
            </Box>
            <Typography variant="body2" sx={{ wordBreak: 'break-word' }}>
              {record.description}
            </Typography>
          </Box>
        )}

        {/* Names */}
        {record.names && record.names.length > 0 && (
          <Box mb={3}>
            <Box display="flex" alignItems="center" mb={1}>
              <PersonIcon sx={{ mr: 1, color: 'text.secondary' }} />
              <Typography variant="subtitle2" fontWeight="bold">
                Names ({record.names.length})
              </Typography>
            </Box>
            <Box display="flex" gap={0.5} flexWrap="wrap">
              {record.names.map((name, index) => (
                <Chip
                  key={index}
                  label={name}
                  size="small"
                  variant="outlined"
                  color="primary"
                />
              ))}
            </Box>
          </Box>
        )}

        {/* Subjects */}
        {record.subjects && record.subjects.length > 0 && (
          <Box mb={3}>
            <Box display="flex" alignItems="center" mb={1}>
              <TagIcon sx={{ mr: 1, color: 'text.secondary' }} />
              <Typography variant="subtitle2" fontWeight="bold">
                Subjects ({record.subjects.length})
              </Typography>
            </Box>
            <Box display="flex" gap={0.5} flexWrap="wrap">
              {record.subjects.map((subject, index) => (
                <Chip
                  key={index}
                  label={subject}
                  size="small"
                  variant="outlined"
                  color="secondary"
                />
              ))}
            </Box>
          </Box>
        )}

        {/* Dates */}
        {record.dates && record.dates.length > 0 && (
          <Box mb={3}>
            <Box display="flex" alignItems="center" mb={1}>
              <DateIcon sx={{ mr: 1, color: 'text.secondary' }} />
              <Typography variant="subtitle2" fontWeight="bold">
                Associated Dates
              </Typography>
            </Box>
            <Typography variant="body2">
              {formatDates(record.dates)}
            </Typography>
          </Box>
        )}

        <Divider sx={{ my: 2 }} />

        {/* Metadata */}
        <Box mb={3}>
          <Typography variant="subtitle2" fontWeight="bold" mb={1}>
            Metadata
          </Typography>
          <List dense>
            <ListItem disablePadding>
              <ListItemText
                primary="Added to Database"
                secondary={formatDate(record.created_at)}
              />
            </ListItem>
            {record.updated_at && (
              <ListItem disablePadding>
                <ListItemText
                  primary="Last Updated"
                  secondary={formatDate(record.updated_at)}
                />
              </ListItem>
            )}
            <ListItem disablePadding>
              <ListItemText
                primary="Content Hash"
                secondary={
                  <Typography
                    variant="caption"
                    sx={{
                      fontFamily: 'monospace',
                      wordBreak: 'break-all',
                    }}
                  >
                    {record.content_hash}
                  </Typography>
                }
              />
            </ListItem>
          </List>
        </Box>

        {/* Actions */}
        <Box display="flex" gap={1} flexWrap="wrap">
          <Button
            variant="contained"
            startIcon={<OpenIcon />}
            onClick={() => window.open(record.url, '_blank')}
            fullWidth
          >
            View in Library of Congress
          </Button>
        </Box>

        {/* Artifacts Section (fetched by block_id) */}
        <Box mb={3}>
          <Typography variant="subtitle2" fontWeight="bold" mb={1}>
            Associated Artifacts
          </Typography>
          {artifactsLoading ? (
            <CircularProgress size={24} />
          ) : artifacts.length > 0 ? (
            <Box display="flex" gap={1} flexWrap="wrap">
              {artifacts.map((artifact) => (
                <Chip
                  key={artifact.artifact_id}
                  label={artifact.title || artifact.artifact_id}
                  onClick={() => setSelectedArtifact(artifact)}
                  color="info"
                  variant="outlined"
                  sx={{ cursor: 'pointer' }}
                />
              ))}
            </Box>
          ) : (
            <Typography variant="body2" color="text.secondary">
              No artifacts found for this block.
            </Typography>
          )}
        </Box>

        {/* Artifact Detail Dialog */}
        <Dialog
          open={!!selectedArtifact}
          onClose={() => setSelectedArtifact(null)}
          maxWidth="sm"
          fullWidth
        >
          <DialogTitle>
            Artifact Details
          </DialogTitle>
          <DialogContent dividers>
            {selectedArtifact && (
              <>
                <Typography variant="h6" gutterBottom>
                  {selectedArtifact.title}
                </Typography>
                {selectedArtifact.image_urls && selectedArtifact.image_urls.length > 0 && (
                  <Box mb={2}>
                    <CardMedia
                      component="img"
                      image={selectedArtifact.image_urls[0]}
                      alt={selectedArtifact.title}
                      sx={{
                        width: '100%',
                        height: 200,
                        objectFit: 'cover',
                        borderRadius: 1,
                        mb: 1,
                      }}
                    />
                  </Box>
                )}
                {selectedArtifact.description && (
                  <Typography variant="body2" mb={2}>
                    {selectedArtifact.description}
                  </Typography>
                )}
                {selectedArtifact.metadata && (
                  <Box>
                    <Typography variant="subtitle2" fontWeight="bold" mb={1}>
                      Metadata
                    </Typography>
                    <pre style={{ fontSize: 12, background: '#f5f5f5', padding: 8, borderRadius: 4 }}>
                      {JSON.stringify(selectedArtifact.metadata, null, 2)}
                    </pre>
                  </Box>
                )}
                {/* Show all artifact fields */}
                <Box mt={2}>
                  <Typography variant="subtitle2" fontWeight="bold" mb={1}>
                    All Artifact Data (Raw)
                  </Typography>
                  <pre style={{ fontSize: 12, background: '#f5f5f5', padding: 8, borderRadius: 4 }}>
                    {JSON.stringify(selectedArtifact, null, 2)}
                  </pre>
                </Box>
              </>
            )}
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setSelectedArtifact(null)} color="primary">
              Close
            </Button>
          </DialogActions>
        </Dialog>

        {/* All Record Data (Raw) */}
        <Box mb={3}>
          <Typography variant="subtitle2" fontWeight="bold" mb={1}>
            All Record Data (Raw)
          </Typography>
          <pre
            style={{
              fontSize: 12,
              background: '#f5f5f5',
              padding: 8,
              borderRadius: 4,
              maxHeight: 300,
              overflow: 'auto',
            }}
          >
            {JSON.stringify(record, null, 2)}
          </pre>
        </Box>
      </CardContent>
    </Card>
  );
};

export default RecordDetail;
