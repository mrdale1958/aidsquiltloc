import React, { useState, useEffect, useCallback } from 'react';
import {
  Container,
  Card,
  CardContent,
  Typography,
  Box,
  TextField,
  InputAdornment,
  Tabs,
  Tab,
  Fade,
  CircularProgress,
  Alert,
} from '@mui/material';
import {
  Search as SearchIcon,
  ViewList as ViewListIcon,
  Analytics as AnalyticsIcon,
} from '@mui/icons-material';
import StatsOverview from './StatsOverview';
import RecordsTable from './RecordsTable';
import RecordDetail from './RecordDetail';
import { QuiltRecord, Stats } from '../types/api';
import { apiService } from '../services/apiService';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`dashboard-tabpanel-${index}`}
      aria-labelledby={`dashboard-tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ py: 3 }}>{children}</Box>}
    </div>
  );
}

function a11yProps(index: number) {
  return {
    id: `dashboard-tab-${index}`,
    'aria-controls': `dashboard-tabpanel-${index}`,
  };
}

const Dashboard: React.FC = () => {
  const [tabValue, setTabValue] = useState(0);
  const [searchQuery, setSearchQuery] = useState('');
  const [stats, setStats] = useState<Stats | null>(null);
  const [records, setRecords] = useState<QuiltRecord[]>([]);
  const [selectedRecord, setSelectedRecord] = useState<QuiltRecord | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [totalRecords, setTotalRecords] = useState(0);
  const [searchLoading, setSearchLoading] = useState(false);

  const loadStats = useCallback(async () => {
    try {
      const statsData = await apiService.getStats();
      setStats(statsData);
    } catch (err) {
      console.error('Error loading stats:', err);
      setError('Failed to load statistics');
    }
  }, []);

  const loadRecords = useCallback(async () => {
    try {
      setLoading(true);
      const response = await apiService.getRecords({ page, pageSize: 20 });
      setRecords(response.records);
      setTotalRecords(response.total);
    } catch (err) {
      console.error('Error loading records:', err);
      setError('Failed to load records');
    } finally {
      setLoading(false);
    }
  }, [page]);

  const performSearch = useCallback(async () => {
    try {
      setSearchLoading(true);
      const response = await apiService.searchRecords({
        query: searchQuery,
        page,
        pageSize: 20,
      });
      setRecords(response.records);
      setTotalRecords(response.total);
    } catch (err) {
      console.error('Error searching records:', err);
      setError('Failed to search records');
    } finally {
      setSearchLoading(false);
    }
  }, [searchQuery, page]);

  // Load initial data
  useEffect(() => {
    loadStats();
    loadRecords();
  }, [loadStats, loadRecords]);

  // Handle search
  useEffect(() => {
    const delayedSearch = setTimeout(() => {
      if (searchQuery.trim()) {
        performSearch();
      } else {
        loadRecords();
      }
    }, 500);

    return () => clearTimeout(delayedSearch);
  }, [searchQuery, page, performSearch, loadRecords]);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const handleRecordSelect = (record: QuiltRecord) => {
    setSelectedRecord(record);
  };

  const handlePageChange = (newPage: number) => {
    setPage(newPage);
  };

  if (error) {
    return (
      <Container maxWidth="xl" sx={{ py: 4 }}>
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      </Container>
    );
  }

  return (
    <Container maxWidth="xl" sx={{ py: 4 }}>
      {/* Search Bar */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <TextField
            fullWidth
            placeholder="Search quilt records by title, names, or description..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  {searchLoading ? (
                    <CircularProgress size={20} />
                  ) : (
                    <SearchIcon />
                  )}
                </InputAdornment>
              ),
            }}
            sx={{
              '& .MuiOutlinedInput-root': {
                borderRadius: 2,
              },
            }}
          />
          {searchQuery && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              {totalRecords} record(s) found for "{searchQuery}"
            </Typography>
          )}
        </CardContent>
      </Card>

      {/* Main Tabs */}
      <Card>
        <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Tabs
            value={tabValue}
            onChange={handleTabChange}
            aria-label="dashboard tabs"
            sx={{ px: 2 }}
          >
            <Tab
              icon={<AnalyticsIcon />}
              label="Overview"
              iconPosition="start"
              {...a11yProps(0)}
            />
            <Tab
              icon={<ViewListIcon />}
              label="Records"
              iconPosition="start"
              {...a11yProps(1)}
            />
          </Tabs>
        </Box>

        {/* Overview Tab */}
        <TabPanel value={tabValue} index={0}>
          <Fade in={tabValue === 0}>
            <div>
              {stats ? (
                <StatsOverview stats={stats} />
              ) : (
                <Box display="flex" justifyContent="center" py={4}>
                  <CircularProgress />
                </Box>
              )}
            </div>
          </Fade>
        </TabPanel>

        {/* Records Tab */}
        <TabPanel value={tabValue} index={1}>
          <Fade in={tabValue === 1}>
            <div>
              <Box
                sx={{
                  display: 'grid',
                  gridTemplateColumns: selectedRecord ? '1fr 400px' : '1fr',
                  gap: 3,
                }}
              >
                <RecordsTable
                  records={records}
                  loading={loading || searchLoading}
                  onRecordSelect={handleRecordSelect}
                  selectedRecord={selectedRecord}
                  page={page}
                  totalRecords={totalRecords}
                  onPageChange={handlePageChange}
                />
                {selectedRecord && (
                  <RecordDetail
                    record={selectedRecord}
                    onClose={() => setSelectedRecord(null)}
                  />
                )}
              </Box>
            </div>
          </Fade>
        </TabPanel>
      </Card>
    </Container>
  );
};

export default Dashboard;
