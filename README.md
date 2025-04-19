# 🌍 Swift Codes API

An API for accessing SWIFT/BIC codes, powered by FastAPI, and PostgreSQL.


## 🚀 Getting Started

### 0. Prerequisites

- [Docker + Docker Compose](https://docs.docker.com/get-docker/)
- Python 3.10+ (for local scripts or development)

---

### 1. Clone the Repository

```bash
git clone https://github.com/bdziurczak/swift-codes-api.git
cd swift-codes-api
```

### 2. Set up virtual environment
```bash
python -m venv .venv
source .venv/bin/activate        # macOS/Linux

.venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### 3. Run project
```
docker compose up --build
```
API should be accessible at http://localhost:8080

To relaunch project run:
```bash
docker compose down -v
docker compose up --build
``` 
### 4. Documentation

API Documentation is available at http://localhost:8080/docs

### 5. Testing
