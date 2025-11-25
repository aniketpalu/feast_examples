# Postgres Setup Instructions

## Files Created

1. `docker-compose.yml` - Postgres container with localhost access
2. `postgres-k8s-service.yaml` - Kubernetes manifests for cluster access

## Setup Steps

### 1. Start Postgres Container

```bash
# Navigate to the directory containing docker-compose.yml
cd "/Users/apaluska/Work/EAP Demo"

# Start Postgres container
docker-compose up -d

# Verify container is running
docker-compose ps
```

### 2. Deploy Kubernetes Resources (for cluster access)

```bash
# Apply the Kubernetes manifests
kubectl apply -f postgres-k8s-service.yaml

# Verify deployment
kubectl get services
kubectl get deployments
kubectl get pods
```

## Connection Details

### Database Credentials
- **Database**: `demo_db`
- **Username**: `demo_user`
- **Password**: `demo_password`
- **Port**: `5432`

## Testing Connectivity

### From Local Terminal

#### Using psql
```bash
# Install psql if not available (macOS)
brew install postgresql

# Connect to Postgres
psql -h localhost -p 5432 -U demo_user -d demo_db
# When prompted, enter password: demo_password
```

#### Using Python
```python
import psycopg2

# Connection parameters
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="demo_db",
    user="demo_user",
    password="demo_password"
)

# Test connection
cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())
cur.close()
conn.close()
```

### From Kind Cluster

#### Method 1: Using ExternalName Service
```bash
# Create a test pod
kubectl run postgres-test --image=postgres:15-alpine --rm -it --restart=Never -- bash

# Inside the pod, connect using the service
psql -h postgres-external -p 5432 -U demo_user -d demo_db
```

#### Method 2: Using NodePort Service
```bash
# Get the cluster node IP
kubectl get nodes -o wide

# Create a test pod
kubectl run postgres-test --image=postgres:15-alpine --rm -it --restart=Never -- bash

# Inside the pod, connect using the NodePort service
psql -h postgres-nodeport -p 5432 -U demo_user -d demo_db
```

#### Method 3: From Application Pod
```yaml
# Example deployment that connects to Postgres
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app-example
spec:
  replicas: 1
  selector:
    matchLabels:
      app: example
  template:
    metadata:
      labels:
        app: example
    spec:
      containers:
      - name: app
        image: postgres:15-alpine
        env:
        - name: PGHOST
          value: "postgres-external"
        - name: PGPORT
          value: "5432"
        - name: PGDATABASE
          value: "demo_db"
        - name: PGUSER
          value: "demo_user"
        - name: PGPASSWORD
          value: "demo_password"
        command: ["sleep", "3600"]
```

## Verification Commands

### Check Container Health
```bash
# Check if Postgres is ready
docker-compose exec postgres pg_isready -U demo_user -d demo_db

# View container logs
docker-compose logs postgres
```

### Check Kubernetes Resources
```bash
# Check service status
kubectl get svc postgres-external postgres-nodeport

# Check proxy deployment
kubectl get deployment postgres-proxy
kubectl get pods -l app=postgres-proxy

# Check proxy logs
kubectl logs -l app=postgres-proxy
```

### Test Database Operations
```sql
-- Create a test table
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO test_table (name) VALUES ('Test Entry 1'), ('Test Entry 2');

-- Query data
SELECT * FROM test_table;
```

## Troubleshooting

### Common Issues

1. **Connection refused from cluster**:
   - Ensure `host.docker.internal` is accessible from kind cluster
   - Check if postgres-proxy pod is running: `kubectl get pods -l app=postgres-proxy`

2. **Port already in use**:
   - Stop any existing Postgres instances: `brew services stop postgresql`
   - Or change the port in docker-compose.yml

3. **Authentication failed**:
   - Verify credentials match those in docker-compose.yml
   - Check container logs: `docker-compose logs postgres`

### Cleanup
```bash
# Stop and remove containers
docker-compose down -v

# Remove Kubernetes resources
kubectl delete -f postgres-k8s-service.yaml
```
