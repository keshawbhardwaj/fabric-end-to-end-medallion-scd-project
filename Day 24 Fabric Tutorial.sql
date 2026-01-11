CREATE TABLE DWH_Stage.Customer_DS (
customer_id            VARCHAR(50),      -- from source (not trusted as unique)
    customer_name          VARCHAR(200),
    gender                 CHAR(1),           -- M / F / U
    dob                    DATE,
    email                  VARCHAR(200),      -- natural business key
    phone                  VARCHAR(20),
    city                   VARCHAR(100),
    state                  VARCHAR(100),
    country                VARCHAR(50),

    last_order_timestamp   DATETIME2(6),         -- max(OrderCreatedTimestamp)
    load_timestamp         DATETIME2(6)
)

CREATE TABLE DWH_Stage.Product_DS(
product_id             VARCHAR(50),
    product_name           VARCHAR(200),
    category               VARCHAR(100),
    sub_category            VARCHAR(100),
    brand                  VARCHAR(100),

    avg_unit_price         DECIMAL(18,2),     -- stabilized price

    last_order_timestamp   DATETIME2(6),
    load_timestamp         DATETIME2(6)
)

CREATE TABLE DWH_Stage.Sales_FS (
order_id               VARCHAR(50),
    order_line_id          INT,
    order_date             DATE,

    customer_id            VARCHAR(50),       -- raw ID
    email                  VARCHAR(200),      -- used for customer join later
    product_id             VARCHAR(50),

    quantity               INT,
    unit_price             DECIMAL(18,2),
    discount_amount        DECIMAL(18,2),

    gross_amount           DECIMAL(18,2),
    net_amount             DECIMAL(18,2),

    sales_channel          VARCHAR(50),
    payment_mode           VARCHAR(50),
    order_status           VARCHAR(50),

    order_created_timestamp DATETIME2(6),

    load_timestamp         DATETIME2(6)
)

CREATE TABLE DWH_Mart.DIM_Customer (
    customer_key    BIGINT,
    customer_id     VARCHAR(50),
    customer_name   VARCHAR(200),
    gender          CHAR(1),
    dob             DATE,
    email           VARCHAR(200),
    phone           VARCHAR(20),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(50),
    load_timestamp  DATETIME2(6)
);

CREATE TABLE DWH_Mart.DIM_Product (
    product_key     BIGINT,
    product_id      VARCHAR(50),
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    sub_category     VARCHAR(100),
    brand           VARCHAR(100),
    avg_unit_price  DECIMAL(18,2),
    load_timestamp  DATETIME2(6)
);

CREATE TABLE DWH_Mart.FACT_Sales_SCD2 (
    sales_key       BIGINT,
    order_id        VARCHAR(50),
    order_line_id   INT,
    order_date      DATE,

    customer_key    BIGINT,
    product_key     BIGINT,

    quantity        INT,
    unit_price      DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    gross_amount    DECIMAL(18,2),
    net_amount      DECIMAL(18,2),

    sales_channel   VARCHAR(50),
    payment_mode    VARCHAR(50),

    effective_from  DATE,
    effective_to    DATE,
    is_current      INT,

    row_hash        VARCHAR(64)
);

CREATE TABLE DWH_Mart.DIM_KEY_CONTROL (
    table_name VARCHAR(100),
    last_key   BIGINT
);

INSERT INTO DWH_Mart.DIM_KEY_CONTROL VALUES
('DIM_Customer', 0),
('DIM_Product', 0),
('FACT_Sales_SCD2', 0);


ALTER TABLE DWH_Stage.Sales_FS ADD row_hash VARCHAR(64)


=========================================================================================================================

-- Surrogate Key Variable
DECLARE @CustomerKey BIGINT;

SELECT @CustomerKey = last_key
FROM DWH_Mart.DIM_KEY_CONTROL
WHERE table_name = 'DIM_Customer';

--Merge Operation

MERGE DWH_Mart.DIM_Customer AS tgt
USING (
    SELECT
        customer_id,
        customer_name,
        gender,
        dob,
        email,
        phone,
        city,
        state,
        country,
        ROW_NUMBER() OVER (ORDER BY email) + @CustomerKey AS new_customer_key
    FROM DWH_Stage.Customer_DS
) AS src
ON tgt.email = src.email

WHEN MATCHED THEN
    UPDATE SET
        tgt.customer_name = src.customer_name,
        tgt.gender = src.gender,
        tgt.dob = src.dob,
        tgt.phone = src.phone,
        tgt.city = src.city,
        tgt.state = src.state,
        tgt.country = src.country,
        tgt.load_timestamp = SYSUTCDATETIME()

WHEN NOT MATCHED THEN
    INSERT (
        customer_key,
        customer_id,
        customer_name,
        gender,
        dob,
        email,
        phone,
        city,
        state,
        country,
        load_timestamp
    )
    VALUES (
        src.new_customer_key,
        src.customer_id,
        src.customer_name,
        src.gender,
        src.dob,
        src.email,
        src.phone,
        src.city,
        src.state,
        src.country,
        SYSUTCDATETIME()
    );



--Update Control Table

UPDATE DWH_Mart.DIM_KEY_CONTROL
SET last_key = (
    SELECT MAX(customer_key)
    FROM DWH_Mart.DIM_Customer
)
WHERE table_name = 'DIM_Customer';


========================================================================================================

DECLARE @ProductKey BIGINT;

SELECT @ProductKey = last_key
FROM DWH_Mart.DIM_KEY_CONTROL
WHERE table_name = 'DIM_Product';


MERGE DWH_Mart.DIM_Product AS tgt
USING (
    SELECT
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        avg_unit_price,
        ROW_NUMBER() OVER (ORDER BY product_id) + @ProductKey AS new_product_key
    FROM DWH_Stage.Product_DS
) AS src
ON tgt.product_id = src.product_id

WHEN MATCHED THEN
    UPDATE SET
        tgt.product_name   = src.product_name,
        tgt.category       = src.category,
        tgt.sub_category   = src.sub_category,
        tgt.brand          = src.brand,
        tgt.avg_unit_price = src.avg_unit_price,
        tgt.load_timestamp = SYSUTCDATETIME()

WHEN NOT MATCHED THEN
    INSERT (
        product_key,
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        avg_unit_price,
        load_timestamp
    )
    VALUES (
        src.new_product_key,
        src.product_id,
        src.product_name,
        src.category,
        src.sub_category,
        src.brand,
        src.avg_unit_price,
        SYSUTCDATETIME()
    );


UPDATE DWH_Mart.DIM_KEY_CONTROL
SET last_key = (
    SELECT MAX(product_key)
    FROM DWH_Mart.DIM_Product
)
WHERE table_name = 'DIM_Product';


======================================================================================================

UPDATE DWH_Stage.Sales_FS
SET row_hash =
    CONVERT(
        VARCHAR(64),
        HASHBYTES(
            'SHA2_256',
            CONCAT(
                order_id,
                order_line_id,
                quantity,
                unit_price,
                discount_amount,
                gross_amount,
                net_amount
            )
        ),
        2
    );


=========================
DECLARE @SalesKey BIGINT;

SELECT @SalesKey = last_key
FROM DWH_Mart.DIM_KEY_CONTROL
WHERE table_name = 'FACT_Sales_SCD2';


MERGE DWH_Mart.FACT_Sales_SCD2 AS tgt
USING DWH_Stage.Sales_FS AS src
ON tgt.order_id = src.order_id
AND tgt.order_line_id = src.order_line_id
AND tgt.is_current = 1

WHEN MATCHED
AND tgt.row_hash <> src.row_hash
THEN
    UPDATE SET
        tgt.effective_to = CAST(GETDATE() AS DATE),
        tgt.is_current = 0;


INSERT INTO DWH_Mart.FACT_Sales_SCD2
(
    sales_key,
    order_id,
    order_line_id,
    order_date,
    customer_key,
    product_key,
    quantity,
    unit_price,
    discount_amount,
    gross_amount,
    net_amount,
    sales_channel,
    payment_mode,
    effective_from,
    effective_to,
    is_current,
    row_hash
)
SELECT
    ROW_NUMBER() OVER (ORDER BY src.order_id, src.order_line_id) + @SalesKey AS sales_key,
    src.order_id,
    src.order_line_id,
    src.order_date,
    c.customer_key,
    p.product_key,
    src.quantity,
    src.unit_price,
    src.discount_amount,
    src.gross_amount,
    src.net_amount,
    src.sales_channel,
    src.payment_mode,
    CAST(GETDATE() AS DATE),
    '9999-12-31',
    1,
    src.row_hash
FROM DWH_Stage.Sales_FS src
JOIN DWH_Mart.DIM_Customer c
    ON src.email = c.email
JOIN DWH_Mart.DIM_Product p
    ON src.product_id = p.product_id
WHERE NOT EXISTS (
    SELECT 1
    FROM DWH_Mart.FACT_Sales_SCD2 tgt
    WHERE tgt.order_id = src.order_id
      AND tgt.order_line_id = src.order_line_id
      AND tgt.is_current = 1
      AND tgt.row_hash = src.row_hash
);


UPDATE DWH_Mart.DIM_KEY_CONTROL
SET last_key = (
    SELECT MAX(sales_key)
    FROM DWH_Mart.FACT_Sales_SCD2
)
WHERE table_name = 'FACT_Sales_SCD2';


