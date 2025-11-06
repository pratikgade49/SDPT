-- Smart Demand Planning Tool - Database Initialization Script
-- Creates the public schema tables for tenant management

-- Create public.tenants table
CREATE TABLE IF NOT EXISTS public.tenants (
    tenant_id UUID PRIMARY KEY,
    tenant_name VARCHAR(255) NOT NULL,
    tenant_identifier VARCHAR(100) UNIQUE NOT NULL,
    admin_email VARCHAR(255) NOT NULL,
    admin_password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'ACTIVE',
    CONSTRAINT check_status CHECK (status IN ('ACTIVE', 'INACTIVE', 'SUSPENDED'))
);

-- Create index on tenant_identifier for faster lookups
CREATE INDEX IF NOT EXISTS idx_tenants_identifier ON public.tenants(tenant_identifier);
CREATE INDEX IF NOT EXISTS idx_tenants_email ON public.tenants(admin_email);

-- Create audit log table (optional but recommended)
CREATE TABLE IF NOT EXISTS public.audit_log (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID REFERENCES public.tenants(tenant_id) ON DELETE CASCADE,
    action VARCHAR(50) NOT NULL,
    entity_type VARCHAR(100),
    entity_id UUID,
    performed_by VARCHAR(255),
    performed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

CREATE INDEX IF NOT EXISTS idx_audit_tenant ON public.audit_log(tenant_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON public.audit_log(performed_at DESC);

-- Comments for documentation
COMMENT ON TABLE public.tenants IS 'Master table storing all tenant information';
COMMENT ON TABLE public.audit_log IS 'Audit trail for all tenant operations';