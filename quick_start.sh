#!/bin/bash

# Quick start script for Feast example
echo "🚀 Setting up Feast minimal example..."

# Step 1: Start PostgreSQL
echo ""
echo "📊 Step 1: Starting PostgreSQL..."
cd ../postgres
docker-compose up -d
sleep 2

# Step 2: Setup sample data
echo ""
echo "📝 Step 2: Setting up sample data..."
cd ../feast_example_minimal
python setup_sample_data.py

# Step 3: Apply Feast definitions
echo ""
echo "🔧 Step 3: Applying Feast feature definitions..."
feast apply

# Step 4: Materialize features for online serving
echo ""
echo "⚡ Step 4: Materializing features (for online serving)..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    feast materialize-incremental $(date -u -v-7d +%Y-%m-%dT%H:%M:%S) $(date -u +%Y-%m-%dT%H:%M:%S)
else
    # Linux
    feast materialize-incremental $(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%S) $(date -u +%Y-%m-%dT%H:%M:%S)
fi

# Step 5: Test historical retrieval
echo ""
echo "🧪 Step 5: Testing historical feature retrieval..."
python test_historical_retrieval.py

echo ""
echo "✅ Setup complete! You can now test historical feature retrieval."
echo ""
echo "To run again: cd feast_example_minimal && python test_historical_retrieval.py"

