import os
import boto3

# Configuration
MINIO_ENDPOINT = "http://minio:9000" 
MINIO_ACCESS_KEY = "admin"           
MINIO_SECRET_KEY = "password123"     
MINIO_BUCKET = "formula1"
LOCAL_DIR = "/app/rules_ui_states"   # Path inside the Streamlit container

def migrate_files():
    # Initialize S3 Client
    s3_client = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    if not os.path.exists(LOCAL_DIR):
        print(f"❌ Local directory {LOCAL_DIR} not found. Are there any files to migrate?")
        return

    files = [f for f in os.listdir(LOCAL_DIR) if f.endswith('.json')]
    
    if not files:
        print("⚠️ No JSON files found to migrate.")
        return

    print(f"Found {len(files)} files. Starting migration...")

    for filename in files:
        local_path = os.path.join(LOCAL_DIR, filename)
        s3_key = f"ui_states/{filename}"
        
        try:
            # Upload the file
            s3_client.upload_file(local_path, MINIO_BUCKET, s3_key)
            print(f"✅ Successfully migrated: {filename} -> {MINIO_BUCKET}/{s3_key}")
        except Exception as e:
            print(f"❌ Failed to migrate {filename}: {e}")

    print("\n🎉 Migration process complete!")

if __name__ == "__main__":
    migrate_files()