create table if not exists receipt_performance (
    month text not null,
    product_id text not null,
    product_name text,
    receipt_qty numeric,
    sales_amount numeric,
    issue_qty numeric,
    issue_amount numeric,
    stock_qty numeric,
    brand text,
    product_category text,
    source_file_name text,
    uploaded_at timestamptz default now(),
    primary key (month, product_id)
);

create table if not exists material_cost (
    month text not null,
    product_id text not null,   -- 코드+색상 조합 (receipt_performance와 동일)
    product_name text,
    material_cost numeric,
    manufacturing_cost numeric,
    manufacturing_ratio numeric,
    series_name text,
    source_file_name text,
    uploaded_at timestamptz default now(),
    primary key (month, product_id)
);

create table if not exists bom_monthly (
    month text not null,
    product_id text not null,
    material_id text not null,
    material_name text,
    material_group text,
    unit_cost numeric,
    unit_qty numeric,
    bom_amount numeric,
    source_file_name text,
    uploaded_at timestamptz default now(),
    primary key (month, product_id, material_id)
);

create table if not exists purchase (
    month text not null,
    material_id text not null,
    material_code text,          -- 순수 자재코드 (색상 제외) — BOM join 키
    material_color text,         -- 색상코드
    material_name text,
    vendor_name text,
    purchase_qty numeric,
    purchase_amount numeric,
    account_type text,
    source_file_name text,
    uploaded_at timestamptz default now(),
    primary key (month, material_id, vendor_name)
);

-- 기존 테이블에 컬럼 추가 (이미 존재하면 무시)
alter table purchase add column if not exists material_code text;
alter table purchase add column if not exists material_color text;

create table if not exists inventory_begin (
    month text not null,
    material_id text not null,
    material_code text,          -- 순수 자재코드 (색상 제외)
    material_color text,         -- 색상코드
    material_name text,
    begin_qty numeric,
    begin_amount numeric,
    avg_unit_cost numeric,
    unit_name text,
    source_file_name text,
    uploaded_at timestamptz default now(),
    primary key (month, material_id)
);

alter table inventory_begin add column if not exists material_code text;
alter table inventory_begin add column if not exists material_color text;

create table if not exists inventory_end (
    month text not null,
    material_id text not null,
    material_code text,          -- 순수 자재코드 (색상 제외)
    material_color text,         -- 색상코드
    material_name text,
    end_qty numeric,
    end_amount numeric,
    avg_unit_cost numeric,
    unit_name text,
    source_file_name text,
    uploaded_at timestamptz default now(),
    primary key (month, material_id)
);

alter table inventory_end add column if not exists material_code text;
alter table inventory_end add column if not exists material_color text;

create table if not exists upload_log (
    upload_id bigint generated always as identity primary key,
    data_month text not null,
    dataset_type text not null,
    source_file_name text not null,
    row_count integer not null,
    status text not null,
    message text,
    uploaded_at timestamptz default now()
);
