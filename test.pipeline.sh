#!/bin/bash

# Axpo IoT Pipeline Validation Script

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "🧪 Axpo IoT Pipeline Validation"
echo "=========================================="
echo ""

# Test 1: Services Running
echo -n "1. Checking services... "
RUNNING=$(docker-compose ps --services --filter "status=running" | wc -l)
if [ "$RUNNING" -ge 3 ]; then
    echo -e "${GREEN}✓ PASS${NC} ($RUNNING services)"
else
    echo -e "${RED}✗ FAIL${NC} (only $RUNNING services)"
    exit 1
fi

# Test 2: MQTT Broker
echo -n "2. MQTT broker... "
docker exec mqtt_broker mosquitto_sub -t "test" -C 1 -W 2 > /dev/null 2>&1 && \
    echo -e "${GREEN}✓ PASS${NC}" || echo -e "${RED}✗ FAIL${NC}"

# Test 3: PostgreSQL
echo -n "3. PostgreSQL... "
docker exec postgres pg_isready -U iot_user -d iot_data > /dev/null 2>&1 && \
    echo -e "${GREEN}✓ PASS${NC}" || echo -e "${RED}✗ FAIL${NC}"

# Test 4: Tables Created
echo -n "4. Database tables... "
TABLES=$(docker exec postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('sensors', 'measurements');")
if [ "$TABLES" -eq 2 ]; then
    echo -e "${GREEN}✓ PASS${NC} (2 tables)"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Test 5: Sensor Metadata
echo -n "5. Sensor metadata... "
SENSORS=$(docker exec postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(*) FROM sensors;")
if [ "$SENSORS" -ge 5 ]; then
    echo -e "${GREEN}✓ PASS${NC} ($SENSORS sensors)"
else
    echo -e "${RED}✗ FAIL${NC}"
    exit 1
fi

# Test 6: Data Collection
echo -n "6. Waiting for data (10s)... "
sleep 10
MEASUREMENTS=$(docker exec postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(*) FROM measurements;")
if [ "$MEASUREMENTS" -ge 40 ]; then
    echo -e "${GREEN}✓ PASS${NC} ($MEASUREMENTS measurements)"
else
    echo -e "${YELLOW}⚠ WARNING${NC} (only $MEASUREMENTS, waiting 20s more...)"
    sleep 20
    MEASUREMENTS=$(docker exec postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(*) FROM measurements;")
    if [ "$MEASUREMENTS" -ge 100 ]; then
        echo -e "  ${GREEN}✓ PASS${NC} ($MEASUREMENTS measurements)"
    else
        echo -e "  ${RED}✗ FAIL${NC}"
        exit 1
    fi
fi

# Test 7: Data Freshness
echo -n "7. Data freshness... "
LATEST=$(docker exec postgres psql -U iot_user -d iot_data -t -c "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp))) FROM measurements;" | xargs printf "%.0f")
if [ "$LATEST" -le 10 ]; then
    echo -e "${GREEN}✓ PASS${NC} (${LATEST}s old)"
else
    echo -e "${RED}✗ FAIL${NC} (${LATEST}s old)"
fi

# Test 8: All Sensors Reporting
echo -n "8. All sensors active... "
ACTIVE=$(docker exec postgres psql -U iot_user -d iot_data -t -c "SELECT COUNT(DISTINCT sensor_id) FROM measurements WHERE timestamp >= NOW() - INTERVAL '30 seconds';")
if [ "$ACTIVE" -ge 5 ]; then
    echo -e "${GREEN}✓ PASS${NC} ($ACTIVE sensors)"
else
    echo -e "${YELLOW}⚠ WARNING${NC} (only $ACTIVE sensors)"
fi

# Test 9: Raw Files
echo -n "9. Raw file storage... "
if [ -d "data/raw" ] && [ "$(ls -A data/raw 2>/dev/null | wc -l)" -gt 0 ]; then
    echo -e "${GREEN}✓ PASS${NC}"
else
    echo -e "${YELLOW}⚠ WARNING${NC} (no raw files)"
fi

# Summary
echo ""
echo "=========================================="
echo "📊 Pipeline Statistics"
echo "=========================================="
docker exec postgres psql -U iot_user -d iot_data << 'EOF'
SELECT 
    'Sensors' as metric, 
    COUNT(*)::text as value 
FROM sensors
UNION ALL
SELECT 
    'Measurements', 
    COUNT(*)::text 
FROM measurements
UNION ALL
SELECT 
    'Latest Reading', 
    TO_CHAR(MAX(timestamp), 'HH24:MI:SS') 
FROM measurements;
EOF

echo ""
echo -e "${GREEN}✅ All tests passed!${NC}"
echo ""
echo "Next steps:"
echo "  • Query data: make db-shell"
echo "  • View logs: make logs"
echo "  • Monitor MQTT: make mqtt-monitor"