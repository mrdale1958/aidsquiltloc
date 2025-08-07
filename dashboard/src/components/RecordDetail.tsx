import React from 'react';
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
            Item ID: {record.item_id}
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
      </CardContent>
    </Card>
  );
};

export default RecordDetail;
