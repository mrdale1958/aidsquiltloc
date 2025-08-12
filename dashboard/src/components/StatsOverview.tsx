import React from 'react';
import {
  Grid,
  Card,
  CardContent,
  Typography,
  Box,
  LinearProgress,
  Chip,
} from '@mui/material';
import {
  Storage as StorageIcon,
  PhotoLibrary as PhotoIcon,
  ViewModule as BlockIcon,
  Dashboard as PanelIcon,
  Timeline as TrendIcon,
  CheckCircle as HealthIcon,
} from '@mui/icons-material';
import { Stats } from '../types/api';

/**
 * Props interface for StatCard component following project type safety standards
 * Implements comprehensive type validation for AIDS Memorial Quilt statistics
 */
interface StatCardProps {
  title: string;
  value: number | undefined;
  icon: React.ReactNode;
  color: string;
  subtitle?: string;
  progress?: number;
}

/**
 * Individual statistic card component with comprehensive error handling
 * Implements safe value formatting following project error resilience guidelines
 * Provides visual representation of AIDS Memorial Quilt collection metrics
 */
const StatCard: React.FC<StatCardProps> = ({ 
  title, 
  value, 
  icon, 
  color, 
  subtitle, 
  progress 
}) => {
  // Safe value handling with comprehensive null/undefined checks per coding standards
  const safeValue = React.useMemo(() => {
    if (value === null || value === undefined || isNaN(value)) {
      return 0;
    }
    return typeof value === 'number' ? value : parseInt(String(value), 10) || 0;
  }, [value]);

  const formattedValue = React.useMemo(() => {
    try {
      return safeValue.toLocaleString();
    } catch (error) {
      console.warn('Error formatting value for AIDS Memorial Quilt statistics:', { 
        title, 
        value, 
        safeValue, 
        error 
      });
      return '0';
    }
  }, [safeValue, value, title]);

  return (
    <Card 
      sx={{ 
        height: '100%',
        position: 'relative',
        overflow: 'hidden',
        transition: 'all 0.3s ease-in-out',
        '&:hover': {
          transform: 'translateY(-2px)',
          boxShadow: 4,
        },
      }}
    >
      <CardContent sx={{ pb: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 48,
              height: 48,
              borderRadius: 2,
              bgcolor: `${color}.light`,
              color: `${color}.main`,
              mr: 2,
            }}
          >
            {icon}
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography 
              variant="h4" 
              component="div" 
              sx={{ 
                fontWeight: 'bold',
                color: 'text.primary',
                lineHeight: 1.2,
              }}
            >
              {formattedValue}
            </Typography>
            <Typography 
              variant="body2" 
              color="text.secondary"
              sx={{ mt: 0.5 }}
            >
              {title}
            </Typography>
          </Box>
        </Box>
        
        {subtitle && (
          <Typography 
            variant="caption" 
            color="text.secondary"
            sx={{ display: 'block', mb: 1 }}
          >
            {subtitle}
          </Typography>
        )}
        
        {typeof progress === 'number' && (
          <Box sx={{ mt: 1 }}>
            <LinearProgress
              variant="determinate"
              value={Math.min(Math.max(progress, 0), 100)}
              sx={{
                height: 6,
                borderRadius: 3,
                bgcolor: 'grey.200',
                '& .MuiLinearProgress-bar': {
                  bgcolor: `${color}.main`,
                  borderRadius: 3,
                },
              }}
            />
            <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block' }}>
              {Math.round(progress)}% complete
            </Typography>
          </Box>
        )}
      </CardContent>
    </Card>
  );
};

/**
 * Props interface for HealthIndicator component
 * Provides type safety for database health status display
 */
interface HealthIndicatorProps {
  health: string;
}

/**
 * Database health status indicator with comprehensive status mapping
 * Follows project naming conventions and provides clear visual feedback
 * Maps AIDS Memorial Quilt database states to user-friendly indicators
 */
const HealthIndicator: React.FC<HealthIndicatorProps> = ({ health }) => {
  const getHealthConfig = React.useCallback((healthStatus: string) => {
    switch (healthStatus?.toLowerCase()) {
      case 'healthy':
        return { color: 'success', label: 'Healthy', icon: <HealthIcon /> };
      case 'limited':
        return { color: 'warning', label: 'Calculated Data', icon: <TrendIcon /> };
      case 'no_panels':
        return { color: 'warning', label: 'No Panels', icon: <TrendIcon /> };
      case 'empty':
        return { color: 'error', label: 'Empty Database', icon: <StorageIcon /> };
      case 'error':
        return { color: 'error', label: 'Database Error', icon: <StorageIcon /> };
      default:
        return { color: 'default', label: 'Unknown', icon: <HealthIcon /> };
    }
  }, []);

  const config = React.useMemo(() => getHealthConfig(health), [health, getHealthConfig]);

  return (
    <Chip
      icon={config.icon}
      label={config.label}
      color={config.color as 'success' | 'warning' | 'error' | 'default'}
      variant="outlined"
      size="small"
    />
  );
};

/**
 * Props interface for StatsOverview component with proper Stats typing
 * Ensures type safety for AIDS Memorial Quilt statistics display
 */
interface StatsOverviewProps {
  stats: Stats;
}

/**
 * Main statistics overview component for AIDS Memorial Quilt dashboard
 * Implements comprehensive error handling and safe data processing per project standards
 * Provides visual analytics for Library of Congress AIDS Memorial Quilt Records collection
 */
const StatsOverview: React.FC<StatsOverviewProps> = ({ stats }) => {
  // Safe stats processing with comprehensive error handling following project guidelines
  const safeStats = React.useMemo(() => {
    if (!stats || typeof stats !== 'object') {
      console.warn('Invalid AIDS Memorial Quilt stats object received:', stats);
      return {
        total_blocks: 0,
        total_panels: 0,
        blocks_with_images: 0,
        recent_blocks: 0,
        database_size_bytes: 0,
        database_health: 'unknown',
        last_updated: null,
      };
    }

    // Safely extract and convert all numeric values with type validation
    return {
      total_blocks: Number(stats.total_blocks) || 0,
      total_panels: Number(stats.total_panels) || 0,
      blocks_with_images: Number(stats.blocks_with_images) || 0,
      recent_blocks: Number(stats.recent_blocks) || 0,
      database_size_bytes: Number(stats.database_size_bytes) || 0,
      database_health: stats.database_health || 'unknown',
      last_updated: stats.last_updated,
    };
  }, [stats]);

  // Calculate derived metrics with safe math operations per performance optimization guidelines
  const derivedMetrics = React.useMemo(() => {
    const { total_blocks, total_panels, blocks_with_images, database_size_bytes } = safeStats;
    
    return {
      averagePanelsPerBlock: total_blocks > 0 ? (total_panels / total_blocks) : 0,
      imageCompletionRate: total_blocks > 0 ? (blocks_with_images / total_blocks) * 100 : 0,
      databaseSizeMB: database_size_bytes / (1024 * 1024),
    };
  }, [safeStats]);

  const isCalculatedData = safeStats.database_health === 'limited';

  return (
    <Box>
      {/* Database Health Indicator following project documentation standards */}
      <Box sx={{ mb: 3, display: 'flex', alignItems: 'center', gap: 2 }}>
        <Typography variant="h6" component="h2">
          AIDS Memorial Quilt Database Status
        </Typography>
        <HealthIndicator health={safeStats.database_health} />
        {safeStats.last_updated && (
          <Typography variant="body2" color="text.secondary">
            {isCalculatedData ? 'Calculated:' : 'Last updated:'} {new Date(safeStats.last_updated).toLocaleDateString()}
          </Typography>
        )}
      </Box>

      {/* Main Stats Grid using Box layout for Material-UI v5 compatibility */}
      <Box sx={{ display: 'grid', gap: 3, gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))' }}>
        {/* Total Blocks - AIDS Memorial Quilt blocks in database */}
        <StatCard
          title="Total Blocks"
          value={safeStats.total_blocks}
          icon={<BlockIcon />}
          color="primary"
          subtitle={isCalculatedData ? "From loaded records" : "AIDS Memorial Quilt blocks in database"}
        />

        {/* Total Panels - Individual memorial panels within blocks */}
        <StatCard
          title="Total Panels"
          value={safeStats.total_panels}
          icon={<PanelIcon />}
          color="secondary"
          subtitle={isCalculatedData ? "Estimated from records" : `Avg: ${derivedMetrics.averagePanelsPerBlock.toFixed(1)} panels/block`}
        />

        {/* Images Available - Digitization progress tracking */}
        <StatCard
          title="Blocks with Images"
          value={safeStats.blocks_with_images}
          icon={<PhotoIcon />}
          color={isCalculatedData ? "warning" : "success"}
          subtitle={isCalculatedData ? "Data unavailable" : "Image digitization progress"}
          progress={isCalculatedData ? 0 : derivedMetrics.imageCompletionRate}
        />

        {/* Database Size - Storage metrics for monitoring */}
        <StatCard
          title="Database Size"
          value={isCalculatedData ? 0 : Math.round(derivedMetrics.databaseSizeMB)}
          icon={<StorageIcon />}
          color="info"
          subtitle={isCalculatedData ? "Data unavailable" : "Megabytes of stored data"}
        />

        {/* Recent Activity - Tracking new additions */}
        <StatCard
          title="Recent Blocks"
          value={safeStats.recent_blocks}
          icon={<TrendIcon />}
          color="warning"
          subtitle={isCalculatedData ? "All loaded records" : "Added in last 30 days"}
        />
      </Box>

      {/* Additional Insights - Comprehensive collection analysis */}
      {safeStats.total_blocks > 0 && (
        <Box sx={{ mt: 4 }}>
          <Typography variant="h6" component="h3" sx={{ mb: 2 }}>
            AIDS Memorial Quilt Collection Insights
          </Typography>
          <Box sx={{ display: 'grid', gap: 2, gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))' }}>
            {/* Coverage Analysis */}
            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle2" color="text.secondary">
                  Coverage
                </Typography>
                <Typography variant="h6">
                  {isCalculatedData ? 'Est.' : ''} {((safeStats.total_panels / Math.max(safeStats.total_blocks, 1)) * 100).toFixed(1)}%
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {isCalculatedData ? 'Estimated panel coverage' : 'Average panel coverage per block'}
                </Typography>
              </CardContent>
            </Card>
            
            {/* Digitization Progress */}
            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle2" color="text.secondary">
                  Digitization
                </Typography>
                <Typography variant="h6">
                  {isCalculatedData ? 'Unknown' : `${derivedMetrics.imageCompletionRate.toFixed(1)}%`}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {isCalculatedData ? 'Data not available' : 'Blocks with digital images'}
                </Typography>
              </CardContent>
            </Card>
            
            {/* Data Quality Assessment */}
            <Card variant="outlined">
              <CardContent>
                <Typography variant="subtitle2" color="text.secondary">
                  Data Quality
                </Typography>
                <Typography variant="h6">
                  {safeStats.database_health === 'healthy' ? 'Excellent' : 
                   safeStats.database_health === 'limited' ? 'Calculated' : 'Needs Work'}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {isCalculatedData ? 'Using calculated metrics' : 'Overall database health'}
                </Typography>
              </CardContent>
            </Card>
          </Box>
        </Box>
      )}
    </Box>
  );
};

export default StatsOverview;
