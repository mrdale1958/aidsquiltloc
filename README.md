# AIDS Memorial Quilt Records - Digital Archive & Dashboard

A comprehensive digital humanities project that scrapes, preserves, and provides access to AIDS Memorial Quilt records from the Library of Congress collection through a modern web dashboard.

## 🏳️‍🌈 Project Overview

This project creates a digital archive of AIDS Memorial Quilt records by:
- **Scraping** metadata and images from the Library of Congress APIs
- **Storing** data in a structured SQLite database with change tracking
- **Providing** a modern React dashboard for exploration and research
- **Preserving** this important historical collection for future generations

## 🚀 Features

### Data Collection
- **Automated scraping** from Library of Congress AIDS Memorial Quilt Records collection
- **Incremental updates** to detect new and changed records
- **Rate-limited API calls** respecting LOC terms of service
- **High-resolution image download** with metadata preservation
- **Error handling and retry logic** for robust data collection

### Database
- **SQLite database** with comprehensive metadata storage
- **Change detection** using content hashing
- **Full-text search** capabilities
- **Statistics tracking** for collection insights

### Web Dashboard
- **Material Design interface** with LOC and AIDS Memorial branding
- **Interactive data table** with search and pagination
- **Detailed record views** with images and metadata
- **Statistics overview** showing collection coverage
- **Responsive design** for all devices

### API Server
- **RESTful FastAPI backend** serving JSON data
- **CORS-enabled** for frontend integration
- **Paginated responses** for efficient data loading
- **Search endpoints** for filtered results

## 📁 Project Structure

```
AIDSQuiltLOC/
├── src/                          # Core Python modules
│   ├── __init__.py
│   ├── database.py              # SQLAlchemy models and database operations
│   ├── loc_api_client.py        # Library of Congress API client
│   ├── image_downloader.py      # Image downloading functionality
│   └── metadata_extractor.py   # Metadata processing utilities
├── config/                      # Configuration management
│   ├── __init__.py
│   └── settings.py             # Application settings
├── dashboard/                   # React frontend application
│   ├── public/                 # Static assets
│   ├── src/                    # React source code
│   │   ├── components/         # React components
│   │   ├── services/          # API service layer
│   │   └── types/             # TypeScript definitions
│   ├── package.json
│   └── tsconfig.json
├── output/                     # Downloaded content
│   ├── images/                # Downloaded quilt images
│   └── metadata/              # Exported metadata files
├── main.py                    # Main scraper application
├── main_enhanced.py           # Enhanced scraper with database
├── cli.py                     # Command-line interface
├── cli_enhanced.py            # Enhanced CLI with database operations
├── api_server.py              # FastAPI backend server
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8+
- Node.js 14+ (for React dashboard)
- Git

### 1. Clone the Repository
```bash
git clone <repository-url>
cd AIDSQuiltLOC
```

### 2. Python Environment Setup
```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
```

### 3. Dashboard Setup
```bash
cd dashboard
npm install
cd ..
```

## 🚀 Usage

### Data Collection

#### Complete Collection Scraping
```bash
# Unlimited scrape - collect entire collection (5,000+ records)
python main_unlimited.py

# Enhanced scraper with limits
python main_enhanced.py

# Comprehensive scraper (100 records)
python main_comprehensive.py

# CLI interface with options
python cli_enhanced.py scrape --limit 100 --download-images
```

#### Database Operations
```bash
# Check database statistics
python cli_enhanced.py stats

# Search records
python cli_enhanced.py search "memorial"

# Export data
python cli_enhanced.py export records.json
```

### Web Dashboard

#### 1. Start the API Server
```bash
# Run FastAPI backend
python api_server.py
# Server runs on http://localhost:8000
```

#### 2. Start the React Dashboard
```bash
cd dashboard
npm start
# Dashboard runs on http://localhost:3000
```

#### 3. Access the Dashboard
Open your browser to `http://localhost:3000` to explore the collection through the interactive dashboard.

## 📊 Dashboard Features

### Overview Tab
- **Collection Statistics**: Total records, image coverage, recent additions
- **Data Quality Metrics**: Completion percentages and collection health
- **Visual Progress Indicators**: Charts showing digitization progress

### Records Tab
- **Interactive Table**: Sortable columns with pagination
- **Real-time Search**: Find records by title, names, or description
- **Detailed Views**: Click any record for comprehensive metadata
- **External Links**: Direct access to original Library of Congress pages

### Search Functionality
- **Full-text search** across all metadata fields
- **Instant results** with highlighting
- **Faceted filtering** by subjects, dates, and contributors

## 🗄️ Database Schema

The SQLite database stores comprehensive metadata:

```sql
CREATE TABLE quilt_records (
    item_id VARCHAR(255) PRIMARY KEY,
    loc_url VARCHAR(500),
    title TEXT,
    description TEXT,
    subject TEXT,              -- JSON array of subjects
    contributor TEXT,          -- JSON array of contributors
    date_created VARCHAR(100),
    location VARCHAR(500),
    quilt_block_number VARCHAR(20),
    memorial_names TEXT,       -- JSON array of memorial names
    panel_maker VARCHAR(500),
    image_urls TEXT,          -- JSON array of image URLs
    content_hash VARCHAR(64), -- Change detection
    first_seen DATETIME,
    last_updated DATETIME,
    images_downloaded BOOLEAN
);
```

## 🔧 API Endpoints

The FastAPI server provides the following endpoints:

- **GET /** - API information and available endpoints
- **GET /stats** - Database statistics and collection metrics
- **GET /records** - Paginated list of records with sorting
- **GET /records/search** - Search records with query parameters
- **GET /records/{item_id}** - Individual record details

### Example API Usage
```bash
# Get statistics
curl http://localhost:8000/stats

# Get records with pagination
curl "http://localhost:8000/records?page=1&page_size=20"

# Search records
curl "http://localhost:8000/records/search?q=memorial&page=1"
```

## 🤝 Contributing

This project follows ethical web scraping practices and respects the Library of Congress terms of service:

- **Rate limiting** prevents server overload
- **User-Agent identification** for transparency
- **Respectful data usage** for educational and research purposes
- **Attribution** maintained to original sources

### Development Guidelines
1. **Follow PEP 8** Python style guidelines
2. **Use type hints** for function parameters and returns
3. **Include docstrings** for all classes and functions
4. **Test thoroughly** before committing changes
5. **Respect API limits** and terms of service

## 📜 License & Attribution

This project is created for educational and research purposes. All AIDS Memorial Quilt records and images remain the property of their respective creators and the Library of Congress.

### Data Sources
- **Library of Congress**: AIDS Memorial Quilt Records Collection
- **AIDS Memorial**: Partnership acknowledgment and branding
- **Public APIs**: Programmatic access to public collections

### Acknowledgments
- Library of Congress for providing public API access
- AIDS Memorial organization for their ongoing preservation work
- Contributors to the original AIDS Memorial Quilt
- Digital humanities community for tools and inspiration

## 🆘 Support

For questions, issues, or contributions:

1. **Check existing issues** in the repository
2. **Create detailed bug reports** with steps to reproduce
3. **Follow coding standards** for pull requests
4. **Test thoroughly** before submitting changes

## 🔄 Recent Updates

- ✅ Full-stack React dashboard with Material Design
- ✅ FastAPI backend with CORS and pagination
- ✅ Enhanced database schema with change tracking
- ✅ Comprehensive error handling and logging
- ✅ Docker-ready configuration (future enhancement)
- ✅ Automated testing suite (future enhancement)

---

*This project honors the memory of those commemorated in the AIDS Memorial Quilt and supports ongoing digital preservation efforts.*
