import React from 'react';
import {
  Card,
  CardContent,
  Typography,
  Box,
  LinearProgress,
  Chip,
} from '@mui/material';
import {
  CollectionsBookmark as RecordsIcon,
  Image as ImageIcon,
  Schedule as RecentIcon,
  Update as UpdateIcon,
} from '@mui/icons-material';
import { Stats } from '../types/api';

interface StatsOverviewProps {
  stats: Stats;
}

interface StatCardProps {
  title: string;
  value: number;
  icon: React.ReactNode;
  color: 'primary' | 'secondary' | 'success' | 'info';
  subtitle?: string;
}

const StatCard: React.FC<StatCardProps> = ({ title, value, icon, color, subtitle }) => {
  return (
    <Card sx={{ height: '100%' }}>
      <CardContent>
        <Box display="flex" alignItems="center" mb={2}>
          <Box
            sx={{
              backgroundColor: `${color}.light`,
              color: `${color}.main`,
              borderRadius: 2,
              p: 1,
              mr: 2,
            }}
          >
            {icon}
          </Box>
          <Typography variant="h6" component="div" flex={1}>
            {title}
          </Typography>
        </Box>
        <Typography variant="h3" component="div" color={`${color}.main`} mb={1}>
          {value.toLocaleString()}
        </Typography>
        {subtitle && (
          <Typography variant="body2" color="text.secondary">
            {subtitle}
          </Typography>
        )}
      </CardContent>
    </Card>
  );
};

const StatsOverview: React.FC<StatsOverviewProps> = ({ stats }) => {
  const imagePercentage = stats.total_records > 0 
    ? (stats.records_with_images / stats.total_records) * 100 
    : 0;

  const formatLastUpdated = (dateString?: string) => {
    if (!dateString) return 'Never';
    
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    
    if (diffDays === 0) return 'Today';
    if (diffDays === 1) return 'Yesterday';
    if (diffDays < 7) return `${diffDays} days ago`;
    
    return date.toLocaleDateString();
  };

  return (
    <Box>
      {/* Main Stats Grid */}
      <Box 
        sx={{ 
          display: 'grid',
          gridTemplateColumns: {
            xs: '1fr',
            sm: 'repeat(2, 1fr)',
            md: 'repeat(4, 1fr)'
          },
          gap: 3,
          mb: 4
        }}
      >
        <StatCard
          title="Total Records"
          value={stats.total_records}
          icon={<RecordsIcon />}
          color="primary"
          subtitle="AIDS Memorial Quilt records"
        />
        <StatCard
          title="With Images"
          value={stats.records_with_images}
          icon={<ImageIcon />}
          color="secondary"
          subtitle={`${imagePercentage.toFixed(1)}% of records`}
        />
        <StatCard
          title="Recent Records"
          value={stats.recent_records}
          icon={<RecentIcon />}
          color="success"
          subtitle="Added in last 30 days"
        />
        <Card sx={{ height: '100%' }}>
          <CardContent>
            <Box display="flex" alignItems="center" mb={2}>
              <Box
                sx={{
                  backgroundColor: 'info.light',
                  color: 'info.main',
                  borderRadius: 2,
                  p: 1,
                  mr: 2,
                }}
              >
                <UpdateIcon />
              </Box>
              <Typography variant="h6" component="div" flex={1}>
                Last Updated
              </Typography>
            </Box>
            <Typography variant="h6" component="div" color="info.main" mb={1}>
              {formatLastUpdated(stats.last_updated)}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Database synchronization
            </Typography>
          </CardContent>
        </Card>
      </Box>

      {/* Collection Progress */}
      <Card>
        <CardContent>
          <Typography variant="h6" mb={3}>
            Collection Overview
          </Typography>
          
          <Box mb={3}>
            <Box display="flex" justifyContent="space-between" alignItems="center" mb={1}>
              <Typography variant="body1">
                Image Coverage
              </Typography>
              <Chip 
                label={`${imagePercentage.toFixed(1)}%`}
                color={imagePercentage > 75 ? 'success' : imagePercentage > 50 ? 'warning' : 'error'}
                size="small"
              />
            </Box>
            <LinearProgress
              variant="determinate"
              value={imagePercentage}
              sx={{
                height: 10,
                borderRadius: 5,
                backgroundColor: 'grey.200',
                '& .MuiLinearProgress-bar': {
                  borderRadius: 5,
                },
              }}
            />
            <Typography variant="body2" color="text.secondary" mt={1}>
              {stats.records_with_images} of {stats.total_records} records have associated images
            </Typography>
          </Box>

          <Box>
            <Typography variant="body1" mb={2}>
              Data Source
            </Typography>
            <Box display="flex" gap={1} flexWrap="wrap">
              <Chip
                label="Library of Congress"
                variant="outlined"
                sx={{ color: 'primary.main', borderColor: 'primary.main' }}
              />
              <Chip
                label="AIDS Memorial Quilt Records"
                variant="outlined"
                sx={{ color: 'secondary.main', borderColor: 'secondary.main' }}
              />
              <Chip
                label="Public API"
                variant="outlined"
              />
            </Box>
          </Box>
        </CardContent>
      </Card>
    </Box>
  );
};

export default StatsOverview;
