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
}

export interface Stats {
  total_records: number;
  records_with_images: number;
  recent_records: number;
  last_updated?: string;
}

export interface RecordsResponse {
  records: QuiltRecord[];
  total: number;
  page: number;
  page_size: number;
}

export interface SearchParams {
  query?: string;
  page?: number;
  pageSize?: number;
}
