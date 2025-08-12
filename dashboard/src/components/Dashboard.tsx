import React, { useState, useEffect, useCallback, useRef } from 'react';
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

/**
 * AIDS Memorial Quilt Records Dashboard Component
 * Implements comprehensive data visualization and search functionality
 * Following project standards for digital humanities research interface
 * Implements error resilience and performance optimization per project guidelines
 */
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
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [isConnected, setIsConnected] = useState(false);

  // Use refs to prevent multiple initializations and track state
  const initializationRef = useRef(false);
  const isInitializingRef = useRef(false);
  const pageSize = 20;

  /**
   * Test API connectivity before loading data
   * Implements health check for AIDS Memorial Quilt API server
   * Memoized to prevent recreation on every render
   */
  const testApiConnection = useCallback(async (): Promise<boolean> => {
    try {
      console.log('AIDS Memorial Quilt Dashboard: Testing API connectivity');
      const isConnected = await apiService.testConnection();
      if (!isConnected) {
        setError('Cannot connect to AIDS Memorial Quilt API server at http://localhost:8000. Please ensure the API server is running.');
        setLoading(false);
        return false;
      }
      console.log('AIDS Memorial Quilt Dashboard: API connection successful');
      setIsConnected(true);
      setError(null);
      return true;
    } catch (err) {
      console.error('AIDS Memorial Quilt Dashboard: API connection test failed:', err);
      setError('API connection test failed. Please check that the server is running on http://localhost:8000');
      setLoading(false);
      return false;
    }
  }, []); // No dependencies - this function is stable

  /**
   * Load AIDS Memorial Quilt statistics from Library of Congress API
   * Implements error resilience and structured logging per project guidelines
   * Creates fallback stats that work with existing StatsOverview component
   */
  const loadStats = useCallback(async (): Promise<void> => {
    try {
      console.log('AIDS Memorial Quilt Dashboard: Loading statistics from LOC API');
      const statsData = await apiService.getStats();
      console.log('AIDS Memorial Quilt Dashboard: Raw stats data received:', statsData);
      setStats(statsData);
      console.log('AIDS Memorial Quilt Dashboard: Statistics loaded successfully');
    } catch (err) {
      console.error('AIDS Memorial Quilt Dashboard: Error loading statistics:', err);
      console.warn('AIDS Memorial Quilt Dashboard: Generating fallback stats from available data');
      
      // Create fallback stats object that matches the Stats interface
      // This allows us to use the existing StatsOverview component
      const fallbackStats = {
        total_blocks: totalRecords || 0,
        total_panels: Math.round((totalRecords || 0) * 2.9), // Estimated based on typical ratios
        blocks_with_images: 0, // Unknown without API
        recent_blocks: totalRecords || 0, // Assume all are recent without date filtering
        database_size_bytes: 0, // Unknown without API
        database_health: totalRecords > 0 ? 'limited' : 'empty',
        last_updated: new Date().toISOString(),
        // Add any other required properties based on your Stats interface
      } as Stats;
      
      setStats(fallbackStats);
      console.log('AIDS Memorial Quilt Dashboard: Using fallback statistics:', fallbackStats);
    }
  }, [totalRecords]); // Include totalRecords for fallback generation

  /**
   * Load AIDS Memorial Quilt records with pagination support
   * Implements comprehensive error handling for LOC API integration
   * Uses correct API service interface with camelCase parameters
   */
  const loadRecords = useCallback(async (targetPage: number): Promise<void> => {
    try {
      console.log(`AIDS Memorial Quilt Dashboard: Loading records page ${targetPage} from LOC API`);
      
      // Use correct API service interface - camelCase parameters
      const response = await apiService.getRecords({
        page: targetPage,
        pageSize, // Use camelCase as expected by DashboardSearchParams interface
      });
      
      console.log('AIDS Memorial Quilt Dashboard: Raw records response:', response);
      console.log('AIDS Memorial Quilt Dashboard: Records array:', response.records);
      console.log('AIDS Memorial Quilt Dashboard: Total records:', response.total);
      
      setRecords(response.records);
      setTotalRecords(response.total);
      setTotalPages(response.total_pages);
      setCurrentPage(response.page);
      console.log(`AIDS Memorial Quilt Dashboard: Loaded ${response.records.length} records from page ${targetPage}`);
    } catch (err) {
      console.error('AIDS Memorial Quilt Dashboard: Error loading records:', err);
      console.error('AIDS Memorial Quilt Dashboard: Error details:', {
        message: err instanceof Error ? err.message : 'Unknown error',
        stack: err instanceof Error ? err.stack : undefined,
      });
      setError('Failed to load AIDS Memorial Quilt records. Please check your connection and try again.');
    }
  }, [pageSize]); // Only depend on pageSize

  /**
   * Search AIDS Memorial Quilt records using Library of Congress API
   * Implements full-text search across titles, descriptions, and metadata
   * Uses correct API service interface with camelCase parameters
   */
  const handleSearch = useCallback(async (): Promise<void> => {
    if (!searchQuery.trim()) {
      console.log('AIDS Memorial Quilt Dashboard: Empty search query, loading all records');
      await loadRecords(1);
      return;
    }

    try {
      setSearchLoading(true);
      setError(null);
      console.log(`AIDS Memorial Quilt Dashboard: Searching for "${searchQuery}" on page ${page}`);
      
      // Use correct API service interface - camelCase parameters
      const response = await apiService.searchRecords({
        query: searchQuery, // Use 'query' parameter as expected by the interface
        page,
        pageSize, // Use camelCase as expected by DashboardSearchParams interface
      });
      
      setRecords(response.records);
      setTotalRecords(response.total);
      setTotalPages(response.total_pages);
      setCurrentPage(response.page);
      console.log(`AIDS Memorial Quilt Dashboard: Found ${response.total} records for search "${searchQuery}"`);
    } catch (err) {
      console.error('AIDS Memorial Quilt Dashboard: Error searching records:', err);
      setError('Search failed for AIDS Memorial Quilt records. Please try again.');
    } finally {
      setSearchLoading(false);
    }
  }, [searchQuery, page, pageSize, loadRecords]); // Include all dependencies for proper memoization

  /**
   * Handle pagination for AIDS Memorial Quilt records
   * Implements efficient page navigation with LOC API integration
   */
  const handlePageChange = useCallback(async (newPage: number): Promise<void> => {
    if (newPage === currentPage) return;
    
    setLoading(true);
    try {
      await loadRecords(newPage);
    } finally {
      setLoading(false);
    }
  }, [currentPage, loadRecords]);

  /**
   * Initialize AIDS Memorial Quilt dashboard data
   * Single initialization effect to prevent infinite re-renders
   * Implements comprehensive error handling and structured logging
   */
  useEffect(() => {
    const initializeData = async (): Promise<void> => {
      // Prevent multiple initializations
      if (initializationRef.current || isInitializingRef.current) {
        console.log('AIDS Memorial Quilt Dashboard: Already initialized or initializing, skipping');
        return;
      }
      
      console.log('AIDS Memorial Quilt Dashboard: Initializing component, testing connection');
      isInitializingRef.current = true;
      
      try {
        // Test API connection first
        const isConnected = await testApiConnection();
        if (!isConnected) {
          return;
        }

        // Load data if connection is successful
        console.log('AIDS Memorial Quilt Dashboard: Connection verified, loading data');
        initializationRef.current = true;
        
        // Load records first, then stats (since stats might use record count as fallback)
        await loadRecords(1);
        await loadStats();
        
        console.log('AIDS Memorial Quilt Dashboard: Initialization completed successfully');
      } catch (err) {
        console.error('AIDS Memorial Quilt Dashboard: Error during data initialization:', err);
        setError('Failed to initialize AIDS Memorial Quilt dashboard. Please try again.');
      } finally {
        setLoading(false);
        isInitializingRef.current = false;
      }
    };

    initializeData();
  }, []); // Empty dependency array - only run once on mount

  /**
   * Handle search with debouncing for better user experience
   * Implements delayed search to reduce API calls to Library of Congress
   * Only runs after initial data load is complete
   */
  useEffect(() => {
    // Don't run search during initial loading
    if (loading || !initializationRef.current) {
      return;
    }

    const delayedSearch = setTimeout(() => {
      if (searchQuery.trim()) {
        handleSearch();
      } else if (records.length === 0) {
        // Only reload if we don't have records (avoid unnecessary API calls)
        loadRecords(1);
      }
    }, 500); // 500ms debounce delay for respectful API usage

    return () => clearTimeout(delayedSearch);
  }, [searchQuery, handleSearch, loadRecords, records.length, loading]); // Add loading dependency

  /**
   * Handle tab navigation within AIDS Memorial Quilt dashboard
   * Provides seamless switching between overview and records views
   */
  const handleTabChange = (event: React.SyntheticEvent, newValue: number): void => {
    setTabValue(newValue);
    console.log(`AIDS Memorial Quilt Dashboard: Switched to tab ${newValue}`);
  };

  /**
   * Handle AIDS Memorial Quilt record selection for detailed view
   * Implements comprehensive record detail display functionality
   */
  const handleRecordSelect = (record: QuiltRecord): void => {
    setSelectedRecord(record);
    console.log(`AIDS Memorial Quilt Dashboard: Selected record ${record.item_id} - ${record.title}`);
  };

  /**
   * Generate comprehensive statistics from available records data
   * Implements fallback analytics when API stats endpoint is unavailable
   * Follows digital humanities research standards for data presentation
   */
  const generateRecordsAnalytics = useCallback(() => {
    if (!records.length) return null;

    try {
      // Analyze available records for meaningful statistics
      const analytics = {
        totalRecords,
        totalPages,
        currentPageRecords: records.length,
        recordsWithTitles: records.filter(record => record.title && record.title.trim()).length,
        recordsWithDescriptions: records.filter(record => record.description && record.description.trim()).length,
        recordsWithDates: records.filter(record => record.dates && record.dates.length > 0).length,
        recordsWithSubjects: records.filter(record => record.subjects && record.subjects.length > 0).length,
        recordsWithNames: records.filter(record => record.names && record.names.length > 0).length,
        recordsWithImages: records.filter(record => record.image_url || record.url).length,
        
        // Extract unique subjects and names for analysis
        uniqueSubjects: new Set(
          records.flatMap(record => record.subjects || [])
            .filter(subject => subject && subject.trim())
        ).size,
        
        uniqueNames: new Set(
          records.flatMap(record => record.names || [])
            .filter(name => name && name.trim())
        ).size,
        
        // Date range analysis
        dateRange: (() => {
          const dates = records
            .flatMap(record => record.dates || [])
            .filter(date => date && date.trim())
            .sort();
          return dates.length > 0 ? {
            earliest: dates[0],
            latest: dates[dates.length - 1],
            total: dates.length
          } : null;
        })(),
        
        lastUpdated: new Date().toISOString()
      };
      
      console.log('AIDS Memorial Quilt Dashboard: Generated analytics from records:', analytics);
      return analytics;
    } catch (err) {
      console.error('AIDS Memorial Quilt Dashboard: Error generating analytics:', err);
      return null;
    }
  }, [records, totalRecords, totalPages]);

  // Error state display with comprehensive error information
  if (error) {
    return (
      <Container maxWidth="xl" sx={{ py: 4 }}>
        <Alert severity="error" sx={{ mb: 3 }}>
          <Typography variant="h6" component="div">
            AIDS Memorial Quilt Dashboard Error
          </Typography>
          {error}
        </Alert>
        <Box textAlign="center" sx={{ mt: 2 }}>
          <button
            onClick={() => {
              setError(null);
              setLoading(true);
              initializationRef.current = false;
              isInitializingRef.current = false;
              window.location.reload();
            }}
            className="bg-red-600 hover:bg-red-700 text-white font-bold py-2 px-4 rounded"
          >
            Try Again
          </button>
        </Box>
      </Container>
    );
  }

  if (!isConnected && loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-red-600 mx-auto mb-4"></div>
          <h2 className="text-xl font-semibold text-gray-900 mb-2">
            Connecting to AIDS Memorial Quilt Database
          </h2>
          <p className="text-gray-600">
            Establishing connection with the Library of Congress archive...
          </p>
        </div>
      </div>
    );
  }

  return (
    <Container maxWidth="xl" sx={{ py: 4 }}>
      {/* AIDS Memorial Quilt Search Interface */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <TextField
            fullWidth
            placeholder="Search AIDS Memorial Quilt records by title, names, or description..."
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
              {totalRecords} AIDS Memorial Quilt record(s) found for "{searchQuery}"
            </Typography>
          )}
        </CardContent>
      </Card>

      {/* Main AIDS Memorial Quilt Dashboard Tabs */}
      <Card>
        <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Tabs
            value={tabValue}
            onChange={handleTabChange}
            aria-label="AIDS Memorial Quilt dashboard tabs"
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

        {/* AIDS Memorial Quilt Overview Tab - Statistics and Analytics */}
        <TabPanel value={tabValue} index={0}>
          <Fade in={tabValue === 0}>
            <div>
              {stats ? (
                <Box>
                  {/* Show info alert when using fallback stats */}
                  {stats.database_health === 'limited' && (
                    <Alert severity="info" sx={{ mb: 3 }}>
                      <Typography variant="h6" component="div">
                        Statistics from Available Data
                      </Typography>
                      <Typography variant="body2">
                        The primary statistics endpoint is unavailable. Displaying calculated statistics from {totalRecords.toLocaleString()} loaded records.
                      </Typography>
                    </Alert>
                  )}
                  <StatsOverview stats={stats} />
                </Box>
              ) : (
                <Box display="flex" flexDirection="column" alignItems="center" py={4}>
                  {loading ? (
                    <>
                      <CircularProgress />
                      <Typography variant="body2" sx={{ mt: 2 }}>
                        Loading AIDS Memorial Quilt statistics...
                      </Typography>
                    </>
                  ) : (
                    <Alert severity="warning">
                      <Typography variant="h6" component="div">
                        Statistics Unavailable
                      </Typography>
                      <Typography variant="body2">
                        Unable to load or generate statistics. Please check that records are available.
                      </Typography>
                    </Alert>
                  )}
                </Box>
              )}
            </div>
          </Fade>
        </TabPanel>

        {/* AIDS Memorial Quilt Records Tab - Searchable Data Table */}
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
