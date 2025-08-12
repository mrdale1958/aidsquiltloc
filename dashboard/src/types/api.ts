/**
 * Type definitions for AIDS Memorial Quilt Records API
 * Following project coding standards with comprehensive type safety
 * Supports Library of Congress API parameter conventions
 */

export interface QuiltRecord {
  id: number;
  item_id: string;
  title: string;
  description?: string;
  subjects?: string[];
  names?: string[];
  dates?: string[];
  url: string;
  image_url?: string;
  image_path?: string;
  content_hash: string;
  created_at: string;
  updated_at?: string;
  panels?: QuiltPanel[];
}

export interface QuiltPanel {
  panel_id: string;
  block_id: string;
  title: string;
  description?: string;
  image_urls: string[];
  scraped_at?: string;
  updated_at?: string;
  metadata?: Record<string, any>;
}

export interface QuiltBlock {
  block_id: string;
  title: string;
  description?: string;
  created_date?: string;
  total_panels: number;
  scraped_at?: string;
  updated_at?: string;
  metadata?: Record<string, any>;
}

/**
 * Enhanced statistics interface matching API server response
 * Ensures type safety for AIDS Memorial Quilt dashboard metrics
 */
export interface Stats {
  total_blocks: number;
  total_panels: number;
  blocks_with_images: number;
  recent_blocks: number;
  database_size_bytes: number;
  database_health: string;
  last_updated?: string;
}

/**
 * Search response interface with pagination support
 * Following LOC API conventions for comprehensive result handling
 */
export interface SearchResponse {
  records: QuiltRecord[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
  has_next: boolean;
  has_previous: boolean;
}

/**
 * Legacy records response interface for backwards compatibility
 * Maintains compatibility with existing AIDS Memorial Quilt API endpoints
 */
export interface RecordsResponse {
  records: QuiltRecord[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
  has_next: boolean;
  has_previous: boolean;
}

/**
 * Search parameters interface for AIDS Memorial Quilt API queries
 * Implements comprehensive search functionality with Library of Congress API conventions
 * Uses snake_case to match Python backend API parameter naming
 */
export interface SearchParams {
  q?: string;              // Search query parameter for LOC API
  page?: number;           // Page number for pagination
  page_size?: number;      // Number of results per page (snake_case for API compatibility)
  sort_by?: string;        // Sort field for results ordering
  sort_order?: 'asc' | 'desc';  // Sort direction
}

/**
 * Dashboard-specific search parameters interface
 * Provides camelCase interface for React components while maintaining API compatibility
 */
export interface DashboardSearchParams {
  query?: string;          // Search query (camelCase for frontend)
  page?: number;           // Page number
  pageSize?: number;       // Page size (camelCase for frontend)
  sortBy?: string;         // Sort field (camelCase for frontend)
  sortOrder?: 'asc' | 'desc';  // Sort direction
}

export interface ApiError {
  detail: string;
  status_code?: number;
}

/**
 * API service configuration interface
 * Following project configuration management patterns for LOC API integration
 */
export interface ApiConfig {
  baseUrl: string;
  timeout: number;
  retryAttempts: number;
  retryDelay: number;
}

/**
 * Dashboard state management interfaces
 * Supporting comprehensive AIDS Memorial Quilt application state with type safety
 */
export interface DashboardState {
  isLoading: boolean;
  error: string | null;
  stats: Stats | null;
  records: QuiltRecord[];
  selectedRecord: QuiltRecord | null;
  searchQuery: string;
  currentPage: number;
  totalPages: number;
}

export interface LoadingState {
  stats: boolean;
  records: boolean;
  search: boolean;
}

export interface ErrorState {
  stats: string | null;
  records: string | null;
  search: string | null;
  network: string | null;
}
