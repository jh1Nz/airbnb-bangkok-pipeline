-- 1. เคลียร์ตารางเดิม (ถ้ามี)
DROP TABLE IF EXISTS fact_listings CASCADE;
DROP TABLE IF EXISTS dim_host CASCADE;
DROP TABLE IF EXISTS dim_host_tier CASCADE;
DROP TABLE IF EXISTS dim_room_type CASCADE;
DROP TABLE IF EXISTS dim_neighbourhood CASCADE;

-- 2. สร้างตาราง Master / Lookup
CREATE TABLE dim_neighbourhood (
    neighbourhood_id INT PRIMARY KEY,
    neighbourhood_name VARCHAR(100) NOT NULL UNIQUE,
    latitude NUMERIC(9, 5) NOT NULL,
    longitude NUMERIC(9, 5) NOT NULL
);

CREATE TABLE dim_room_type (
    room_type_id INT PRIMARY KEY,
    room_type_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE dim_host_tier (
    tier_id INT PRIMARY KEY,
    tier_name VARCHAR(50) NOT NULL UNIQUE,
    min_listings INT NOT NULL,
    max_listings INT,
    tier_description TEXT
);

-- 3. สร้างตาราง dim_host (เชื่อมโยง tier_id)
CREATE TABLE dim_host (
    host_id BIGINT PRIMARY KEY,
    host_name VARCHAR(255) NOT NULL,
    calculated_host_listings_count INT NOT NULL,
    tier_id INT NOT NULL,
    CONSTRAINT fk_host_tier FOREIGN KEY (tier_id) REFERENCES dim_host_tier (tier_id)
);

-- 4. สร้างตาราง fact_listings (เชื่อมโยง FK ทั้ง 3 ตาราง)
CREATE TABLE fact_listings (
    listing_id BIGINT PRIMARY KEY,
    neighbourhood_id INT NOT NULL,
    room_type_id INT NOT NULL,
    host_id BIGINT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    minimum_nights INT NOT NULL,
    number_of_reviews_ltm INT NOT NULL,
    reviews_per_month NUMERIC(5, 2) NOT NULL,
    number_of_reviews INT NOT NULL,
    availability_365 INT NOT NULL,
    CONSTRAINT fk_fact_neighbourhood FOREIGN KEY (neighbourhood_id) REFERENCES dim_neighbourhood (neighbourhood_id),
    CONSTRAINT fk_fact_room_type FOREIGN KEY (room_type_id) REFERENCES dim_room_type (room_type_id),
    CONSTRAINT fk_fact_host FOREIGN KEY (host_id) REFERENCES dim_host (host_id)
);
