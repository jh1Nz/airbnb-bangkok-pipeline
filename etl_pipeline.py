import os
import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
load_dotenv()

# ==============================================================================
# CONFIGURATION & ENVIRONMENT SETUP
# ==============================================================================
# 1. กำหนด Relative Path ภายใน Repository 
FILE_PATH = os.path.join("data", "listings.csv")
QUARANTINE_DIR = "null_data"
QUARANTINE_FILE = os.path.join(QUARANTINE_DIR, "price_null_or_zero.csv")


# 2. ดึง Database URL จาก GitHub Secrets (ผ่าน Environment Variable)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip().strip("\"'")

if not DATABASE_URL:
    raise ValueError("❌ ไม่พบ DATABASE_URL กรุณาตรวจสอบการตั้งค่า Secret หรือ Environment Variable")

# ล้างคำนำหน้ากรณีติดคำสั่งอื่นมา
for prefix in ["DATABASE_URL=", "psql "]:
    if DATABASE_URL.startswith(prefix):
        DATABASE_URL = DATABASE_URL[len(prefix):].strip().strip("\"'")

# บังคับแปลง Dialect ให้ชี้ไปที่ psycopg2 ชัดเจน
if DATABASE_URL.startswith("postgresql+psycopg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

# ตัด channel_binding ออก
DATABASE_URL = DATABASE_URL.replace("&channel_binding=require", "")




# ==============================================================================
# 1. EXTRACT (ดึงข้อมูลจาก Landing Zone)
# ==============================================================================
def extract_data() -> pd.DataFrame:
    print("==================================================================")
    print("🚀 เริ่มต้นกระบวนการ AUTOMATED ETL PIPELINE (Airbnb Bangkok)")
    print("==================================================================")
    print(f"📥 [Step 1/3: Extract] กำลังอ่านข้อมูลดิบจาก: {FILE_PATH} ...")
    
    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(
            f"❌ ไม่พบไฟล์ที่ {FILE_PATH} กรุณาตรวจสอบว่ามีไฟล์ listings.csv อยู่ในโฟลเดอร์ data/ หรือไม่"
        )
    
    df_raw = pd.read_csv(FILE_PATH)
    print(f"✅ โหลดไฟล์สำเร็จ: จำนวนข้อมูลดิบทั้งหมด {len(df_raw):,} แถว")
    return df_raw


# ==============================================================================
# 2. TRANSFORM (คลีนข้อมูล และแปลงเป็น 3NF Snowflake Schema)
# ==============================================================================
def transform_data(df_raw: pd.DataFrame):
    print("\n🔄 [Step 2/3: Transform] กำลังทำความสะอาดข้อมูลและจัดรูป Schema...")

    # 2.1 คัดเลือกเฉพาะ 14 คอลัมน์ที่จำเป็นต่อสถาปัตยกรรมข้อมูล
    selected_columns = [
        'id', 'price', 'minimum_nights', 'number_of_reviews_ltm',
        'reviews_per_month', 'number_of_reviews', 'availability_365',
        'room_type', 'host_id', 'host_name', 'calculated_host_listings_count',
        'neighbourhood', 'latitude', 'longitude'
    ]
    df_selected = df_raw[selected_columns].copy()

    # 2.2 จัดการ reviews_per_month: แทนที่ค่าว่างด้วย 0.0 (ที่พักเปิดใหม่/ยังไม่มีรีวิว)
    df_selected['reviews_per_month'] = df_selected['reviews_per_month'].fillna(0.0)

    # 2.3 จัดการ price: ทำ Data Quarantine คัดแยกแถวที่ Null หรือ <= 0 ออกไปเก็บ
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    null_price_mask = df_selected['price'].isnull() | (df_selected['price'] <= 0)
    null_price_keep = df_selected[null_price_mask].copy()
    
    if len(null_price_keep) > 0:
        null_price_keep.to_csv(QUARANTINE_FILE, index=False)
        print(f"📦 Data Quarantine: แยกข้อมูลราคาไม่สมบูรณ์ {len(null_price_keep):,} แถว ไปที่: {QUARANTINE_FILE}")
    
    df_selected = df_selected[~null_price_mask].copy()

    # 2.4 จัดการ minimum_nights: ตัดค่า Null และค่า <= 0 ออก
    df_selected = df_selected[
        df_selected['minimum_nights'].notnull() & (df_selected['minimum_nights'] > 0)
    ].copy()
    df_selected['minimum_nights'] = df_selected['minimum_nights'].astype(int)

    print(f"📊 จำนวนแถวข้อมูลสุทธิที่ผ่านเกณฑ์คุณภาพ: {len(df_selected):,} แถว")

    # --------------------------------------------------------------------------
    # สร้างตาราง Dimensions ตามโมเดล Snowflake Schema
    # --------------------------------------------------------------------------
    # A. dim_neighbourhood (50 เขต)
    dim_neighbourhood = (
        df_selected.groupby('neighbourhood', as_index=False)
        .agg(latitude=('latitude', 'mean'), longitude=('longitude', 'mean'))
        .sort_values(by='neighbourhood')
        .reset_index(drop=True)
    )
    dim_neighbourhood.rename(columns={'neighbourhood': 'neighbourhood_name'}, inplace=True)
    dim_neighbourhood.insert(0, 'neighbourhood_id', range(1, len(dim_neighbourhood) + 1))

    # B. dim_room_type (4 ประเภท)
    dim_room_type = (
        pd.DataFrame(df_selected['room_type'].dropna().unique(), columns=['room_type_name'])
        .sort_values(by='room_type_name')
        .reset_index(drop=True)
    )
    dim_room_type.insert(0, 'room_type_id', range(1, len(dim_room_type) + 1))

    # C. dim_host
    dim_host = (
        df_selected.groupby('host_id', as_index=False)
        .agg(
            host_name=('host_name', 'first'),
            calculated_host_listings_count=('calculated_host_listings_count', 'max'),
        )
        .sort_values(by='host_id')
        .reset_index(drop=True)
    )
    dim_host['host_name'] = dim_host['host_name'].fillna('Unknown Host')

    # D. dim_host_tier (Outrigger Dimension - 3NF Normalization)
    host_tier_data = [
        {'tier_id': 1, 'tier_name': 'Single Host', 'min_listings': 1, 'max_listings': 1, 'tier_description': 'โฮสต์รายเดี่ยว ปล่อยเช่า 1 ห้องพัก (เจ้าของบ้านรายย่อย)'},
        {'tier_id': 2, 'tier_name': 'Small Scale (2-5)', 'min_listings': 2, 'max_listings': 5, 'tier_description': 'ผู้ประกอบการรายย่อย พอร์ต 2-5 ห้องพัก'},
        {'tier_id': 3, 'tier_name': 'Medium Scale (6-19)', 'min_listings': 6, 'max_listings': 19, 'tier_description': 'ผู้ประกอบการระดับกลาง พอร์ต 6-19 ห้องพัก'},
        {'tier_id': 4, 'tier_name': 'Large/Agency (>=20)', 'min_listings': 20, 'max_listings': None, 'tier_description': 'เอเจนซี่หรือกลุ่มทุนมืออาชีพ พอร์ต 20 ห้องพักขึ้นไป (คู่แข่งรายใหญ่)'}
    ]
    dim_host_tier = pd.DataFrame(host_tier_data)

    def classify_host_tier(count):
        if count == 1:
            return 1
        elif count <= 5:
            return 2
        elif count <= 19:
            return 3
        else:
            return 4

    # แมป tier_id เข้า dim_host เพื่อขจัด Transitive Dependency สู่ 3NF
    dim_host['tier_id'] = dim_host['calculated_host_listings_count'].apply(classify_host_tier)
    dim_host = dim_host[['host_id', 'host_name', 'calculated_host_listings_count', 'tier_id']].sort_values(by='host_id').reset_index(drop=True)

    # --------------------------------------------------------------------------
    # สร้างตาราง fact_listings (ตารางข้อเท็จจริง)
    # --------------------------------------------------------------------------
    fact_listings = df_selected.merge(
        dim_neighbourhood[['neighbourhood_name', 'neighbourhood_id']],
        left_on='neighbourhood',
        right_on='neighbourhood_name',
        how='inner'
    )
    fact_listings = fact_listings.merge(
        dim_room_type[['room_type_name', 'room_type_id']],
        left_on='room_type',
        right_on='room_type_name',
        how='inner'
    )
    fact_listings.rename(columns={'id': 'listing_id'}, inplace=True)

    fact_columns = [
        'listing_id', 'neighbourhood_id', 'room_type_id', 'host_id',
        'price', 'minimum_nights', 'number_of_reviews_ltm',
        'reviews_per_month', 'number_of_reviews', 'availability_365'
    ]
    fact_listings = fact_listings[fact_columns].copy()

    # --------------------------------------------------------------------------
    # 2.5 Data Type Casting ตาม PostgreSQL Specifications
    # --------------------------------------------------------------------------
    dim_neighbourhood['neighbourhood_id'] = dim_neighbourhood['neighbourhood_id'].astype(int)
    dim_neighbourhood['neighbourhood_name'] = dim_neighbourhood['neighbourhood_name'].astype(str)
    dim_neighbourhood['latitude'] = dim_neighbourhood['latitude'].astype(float).round(5)
    dim_neighbourhood['longitude'] = dim_neighbourhood['longitude'].astype(float).round(5)

    dim_room_type['room_type_id'] = dim_room_type['room_type_id'].astype(int)
    dim_room_type['room_type_name'] = dim_room_type['room_type_name'].astype(str)

    dim_host_tier['tier_id'] = dim_host_tier['tier_id'].astype(int)
    dim_host_tier['tier_name'] = dim_host_tier['tier_name'].astype(str)
    dim_host_tier['min_listings'] = dim_host_tier['min_listings'].astype(int)
    dim_host_tier['max_listings'] = dim_host_tier['max_listings'].astype('Int64')
    dim_host_tier['tier_description'] = dim_host_tier['tier_description'].astype(str)

    dim_host['host_id'] = dim_host['host_id'].astype('int64')
    dim_host['host_name'] = dim_host['host_name'].astype(str)
    dim_host['calculated_host_listings_count'] = dim_host['calculated_host_listings_count'].astype(int)
    dim_host['tier_id'] = dim_host['tier_id'].astype(int)

    fact_listings['listing_id'] = fact_listings['listing_id'].astype('int64')
    fact_listings['neighbourhood_id'] = fact_listings['neighbourhood_id'].astype(int)
    fact_listings['room_type_id'] = fact_listings['room_type_id'].astype(int)
    fact_listings['host_id'] = fact_listings['host_id'].astype('int64')
    fact_listings['price'] = fact_listings['price'].astype(float).round(2)
    fact_listings['minimum_nights'] = fact_listings['minimum_nights'].astype(int)
    fact_listings['number_of_reviews_ltm'] = fact_listings['number_of_reviews_ltm'].astype(int)
    fact_listings['reviews_per_month'] = fact_listings['reviews_per_month'].astype(float).round(2)
    fact_listings['number_of_reviews'] = fact_listings['number_of_reviews'].astype(int)
    fact_listings['availability_365'] = fact_listings['availability_365'].astype(int)

    # --------------------------------------------------------------------------
    # Data Integrity Assertion Checks
    # --------------------------------------------------------------------------
    assert dim_neighbourhood['neighbourhood_id'].is_unique, "PK Error: dim_neighbourhood"
    assert dim_room_type['room_type_id'].is_unique, "PK Error: dim_room_type"
    assert dim_host_tier['tier_id'].is_unique, "PK Error: dim_host_tier"
    assert dim_host['host_id'].is_unique, "PK Error: dim_host"
    assert fact_listings['listing_id'].is_unique, "PK Error: fact_listings"

    assert set(dim_host['tier_id']).issubset(set(dim_host_tier['tier_id'])), "FK Error: tier_id"
    assert set(fact_listings['neighbourhood_id']).issubset(set(dim_neighbourhood['neighbourhood_id'])), "FK Error: neighbourhood_id"
    assert set(fact_listings['room_type_id']).issubset(set(dim_room_type['room_type_id'])), "FK Error: room_type_id"
    assert set(fact_listings['host_id']).issubset(set(dim_host['host_id'])), "FK Error: host_id"
    assert fact_listings[['listing_id', 'neighbourhood_id', 'room_type_id', 'host_id', 'price']].isnull().sum().sum() == 0, "Null Column Error in Fact"

    print("✅ ผ่านการตรวจสอบ Data Integrity 100% ครบทั้ง 5 ตาราง")
    return dim_neighbourhood, dim_room_type, dim_host_tier, dim_host, fact_listings


# ==============================================================================
# 3. LOAD (บรรจุข้อมูลขึ้นสู่ Neon Data Warehouse)
# ==============================================================================
def load_data_to_neon(dim_neighbourhood, dim_room_type, dim_host_tier, dim_host, fact_listings):
    print("\n🚀 [Step 3/3: Load] กำลังเชื่อมต่อและโหลดข้อมูลขึ้น Neon PostgreSQL...")
    engine = create_engine(DATABASE_URL)

    with engine.begin() as conn:
        # เคลียร์ข้อมูลเดิมเพื่อรองรับ Idempotency (รันซ้ำได้โดยข้อมูลไม่เบิ้ล)
        conn.execute(text("TRUNCATE TABLE fact_listings CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_host CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_host_tier CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_room_type CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_neighbourhood CASCADE;"))
    print("🧹 เคลียร์ข้อมูลเดิมในตารางเรียบร้อย")

    # โหลดข้อมูลตามลำดับ DAG
    dim_neighbourhood.to_sql('dim_neighbourhood', engine, if_exists='append', index=False)
    print(f"✅ โหลด dim_neighbourhood สำเร็จ ({len(dim_neighbourhood)} แถว)")

    dim_room_type.to_sql('dim_room_type', engine, if_exists='append', index=False)
    print(f"✅ โหลด dim_room_type สำเร็จ ({len(dim_room_type)} แถว)")

    dim_host_tier.to_sql('dim_host_tier', engine, if_exists='append', index=False)
    print(f"✅ โหลด dim_host_tier สำเร็จ ({len(dim_host_tier)} แถว)")

    dim_host.to_sql('dim_host', engine, if_exists='append', index=False)
    print(f"✅ โหลด dim_host สำเร็จ ({len(dim_host):,} แถว)")

    fact_listings.to_sql('fact_listings', engine, if_exists='append', index=False, chunksize=5000)
    print(f"✅ โหลด fact_listings สำเร็จ ({len(fact_listings):,} แถว)")

    # ยืนยันยอดปลายทาง
    with engine.connect() as conn:
        count_fact = conn.execute(text("SELECT COUNT(*) FROM fact_listings;")).scalar()
        print(f"\n🎯 ยืนยันยอดข้อมูลจริงบน Neon DW: {count_fact:,} แถว (เป้าหมาย: 28,661 แถว)")


# ==============================================================================
# MAIN ENTRY POINT
# ==============================================================================
if __name__ == "__main__":
    df_raw = extract_data()
    dim_neighbourhood, dim_room_type, dim_host_tier, dim_host, fact_listings = transform_data(df_raw)
    load_data_to_neon(dim_neighbourhood, dim_room_type, dim_host_tier, dim_host, fact_listings)
    print("\n🎉 จบกระบวนการ Automated ETL Pipeline สำเร็จสมบูรณ์ 100%!")
