-- Enable logical replication for Debezium CDC
ALTER TABLE organizations REPLICA IDENTITY FULL;
ALTER TABLE sites REPLICA IDENTITY FULL;
ALTER TABLE assets REPLICA IDENTITY FULL;
ALTER TABLE planned_outages REPLICA IDENTITY FULL;

-- Create logical publication
CREATE PUBLICATION registry_publication FOR TABLE organizations, sites, assets, planned_outages;
