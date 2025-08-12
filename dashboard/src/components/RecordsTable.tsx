import React from 'react';
import {
  Card,
  CardContent,
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Chip,
  CircularProgress,
  Avatar,
  IconButton,
  Tooltip,
} from '@mui/material';
import {
  Image as ImageIcon,
  OpenInNew as OpenIcon,
  Visibility as ViewIcon,
} from '@mui/icons-material';
import { QuiltRecord } from '../types/api';

interface RecordsTableProps {
  records: QuiltRecord[];
  loading: boolean;
  onRecordSelect: (record: QuiltRecord) => void;
  selectedRecord: QuiltRecord | null;
  page: number;
  totalRecords: number;
  onPageChange: (page: number) => void;
}

const RecordsTable: React.FC<RecordsTableProps> = ({
  records,
  loading,
  onRecordSelect,
  selectedRecord,
  page,
  totalRecords,
  onPageChange,
}) => {
  const rowsPerPage = 20;

  const handleChangePage = (event: unknown, newPage: number) => {
    onPageChange(newPage + 1); // Convert to 1-based page
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString();
  };

  const getSubjectsDisplay = (subjects?: string[]) => {
    if (!subjects || subjects.length === 0) return '-';
    
    const maxDisplay = 2;
    const displaySubjects = subjects.slice(0, maxDisplay);
    const hasMore = subjects.length > maxDisplay;
    
    return (
      <Box display="flex" gap={0.5} flexWrap="wrap">
        {displaySubjects.map((subject, index) => (
          <Chip
            key={index}
            label={subject}
            size="small"
            variant="outlined"
            sx={{ fontSize: '0.75rem' }}
          />
        ))}
        {hasMore && (
          <Chip
            label={`+${subjects.length - maxDisplay}`}
            size="small"
            variant="outlined"
            color="primary"
            sx={{ fontSize: '0.75rem' }}
          />
        )}
      </Box>
    );
  };

  const getNamesDisplay = (names?: string[]) => {
    if (!names || names.length === 0) return '-';
    return names.slice(0, 2).join(', ') + (names.length > 2 ? ` (+${names.length - 2})` : '');
  };

  if (loading) {
    return (
      <Card>
        <CardContent>
          <Box display="flex" justifyContent="center" alignItems="center" py={8}>
            <CircularProgress />
            <Typography variant="body1" sx={{ ml: 2 }}>
              Loading records...
            </Typography>
          </Box>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardContent>
        <Box display="flex" justifyContent="space-between" alignItems="center" mb={2}>
          <Typography variant="h6">
            Quilt Records
          </Typography>
          <Typography variant="body2" color="text.secondary">
            {totalRecords} total records
          </Typography>
        </Box>

        <TableContainer sx={{ maxHeight: 600 }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow>
                <TableCell>Image</TableCell>
                <TableCell>Title</TableCell>
                <TableCell>Names</TableCell>
                <TableCell>Subjects</TableCell>
                <TableCell>Date Added</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {records.map((record) => (
                <TableRow
                  key={record.id}
                  hover
                  selected={selectedRecord?.id === record.id}
                  sx={{
                    cursor: 'pointer',
                    '&.Mui-selected': {
                      backgroundColor: 'primary.light',
                      '&:hover': {
                        backgroundColor: 'primary.light',
                      },
                    },
                  }}
                  onClick={() => onRecordSelect(record)}
                >
                  <TableCell>
                    <Avatar
                      src={record.image_path}
                      sx={{
                        width: 48,
                        height: 48,
                        bgcolor: record.image_path ? 'transparent' : 'grey.300',
                      }}
                    >
                      {!record.image_path && <ImageIcon />}
                    </Avatar>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" fontWeight="medium" noWrap>
                      {record.title}
                    </Typography>
                    <Typography variant="caption" color="text.secondary" noWrap>
                      ID: {record.item_id}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2" noWrap>
                      {getNamesDisplay(record.names)}
                    </Typography>
                  </TableCell>
                  <TableCell>
                    {getSubjectsDisplay(record.subjects)}
                  </TableCell>
                  <TableCell>
                    <Typography variant="body2">
                      {formatDate(record.created_at)}
                    </Typography>
                  </TableCell>
                  <TableCell align="center">
                    <Box display="flex" gap={1}>
                      <Tooltip title="View Details">
                        <IconButton
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            onRecordSelect(record);
                          }}
                        >
                          <ViewIcon />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title="Open in Library of Congress">
                        <IconButton
                          size="small"
                          onClick={(e) => {
                            e.stopPropagation();
                            window.open(record.url, '_blank');
                          }}
                        >
                          <OpenIcon />
                        </IconButton>
                      </Tooltip>
                    </Box>
                  </TableCell>
                </TableRow>
              ))}
              {records.length === 0 && (
                <TableRow>
                  <TableCell colSpan={6}>
                    <Box display="flex" justifyContent="center" py={4}>
                      <Typography variant="body1" color="text.secondary">
                        No records found
                      </Typography>
                    </Box>
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>

        <TablePagination
          component="div"
          count={totalRecords}
          page={page - 1} // Convert to 0-based for Material-UI
          onPageChange={handleChangePage}
          rowsPerPage={rowsPerPage}
          rowsPerPageOptions={[rowsPerPage]}
        />
      </CardContent>
    </Card>
  );
};

export default RecordsTable;
