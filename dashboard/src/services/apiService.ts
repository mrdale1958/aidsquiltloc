import { QuiltRecord, Stats, RecordsResponse, SearchParams } from '../types/api';

const API_BASE_URL = 'http://localhost:8000';

class ApiService {
  private async fetchWithError(url: string, options?: RequestInit): Promise<any> {
    try {
      const response = await fetch(url, {
        headers: {
          'Content-Type': 'application/json',
          ...options?.headers,
        },
        ...options,
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      return await response.json();
    } catch (error) {
      console.error(`API Error for ${url}:`, error);
      throw error;
    }
  }

  async getStats(): Promise<Stats> {
    return this.fetchWithError(`${API_BASE_URL}/stats`);
  }

  async getRecords(params: SearchParams = {}): Promise<RecordsResponse> {
    const queryParams = new URLSearchParams();
    
    if (params.page) queryParams.append('page', params.page.toString());
    if (params.pageSize) queryParams.append('page_size', params.pageSize.toString());

    const url = `${API_BASE_URL}/records${queryParams.toString() ? `?${queryParams.toString()}` : ''}`;
    return this.fetchWithError(url);
  }

  async searchRecords(params: SearchParams): Promise<RecordsResponse> {
    const queryParams = new URLSearchParams();
    
    if (params.query) queryParams.append('q', params.query);
    if (params.page) queryParams.append('page', params.page.toString());
    if (params.pageSize) queryParams.append('page_size', params.pageSize.toString());

    const url = `${API_BASE_URL}/records/search?${queryParams.toString()}`;
    return this.fetchWithError(url);
  }

  async getRecord(id: number): Promise<QuiltRecord> {
    return this.fetchWithError(`${API_BASE_URL}/records/${id}`);
  }
}

export const apiService = new ApiService();
