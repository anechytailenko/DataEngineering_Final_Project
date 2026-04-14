-- OLTP schema for the logistics domain.
-- Picked up automatically by postgres-oltp on first start
-- (/docker-entrypoint-initdb.d/). Data is loaded separately by
-- scripts/generate_oltp_data.py.

CREATE TABLE IF NOT EXISTS facilities (
    facility_id          SERIAL      PRIMARY KEY,
    facility_name        VARCHAR(100) NOT NULL,
    facility_type        VARCHAR(50)  NOT NULL,
    city                 VARCHAR(100) NOT NULL,
    max_capacity_per_day INTEGER      NOT NULL
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id             VARCHAR(36)    PRIMARY KEY,
    sender_id               INTEGER,
    origin_facility_id      INTEGER        NOT NULL REFERENCES facilities(facility_id),
    destination_facility_id INTEGER        NOT NULL REFERENCES facilities(facility_id),
    weight_kg               DECIMAL(5, 2)  NOT NULL,
    declared_value          DECIMAL(10, 2) NOT NULL,
    shipping_cost           DECIMAL(10, 2) NOT NULL,
    created_at              TIMESTAMP      NOT NULL,
    estimated_delivery_date DATE           NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_shipments_created_at ON shipments (created_at);
CREATE INDEX IF NOT EXISTS idx_shipments_origin     ON shipments (origin_facility_id);
CREATE INDEX IF NOT EXISTS idx_shipments_dest       ON shipments (destination_facility_id);
