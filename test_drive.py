import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def test_oauth_upload():
    print("--- OAuth 2.0 接続テスト開始 ---")
    
    creds = Credentials(
        token=None,
        refresh_token=os.environ.get('REFRESH_TOKEN'),
        client_id=os.environ.get('CLIENT_ID'),
        client_secret=os.environ.get('CLIENT_SECRET'),
        token_uri="https://oauth2.googleapis.com/token"
    )

    try:
        service = build('drive', 'v3', credentials=creds)
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')

        # 1. テストファイル作成
        test_file = 'oauth_success_test.txt'
        with open(test_file, 'w') as f:
            f.write("Google One storage access successful via OAuth 2.0!")

        # 2. アップロード実行
        file_metadata = {'name': test_file, 'parents': [folder_id]}
        media = MediaFileUpload(test_file, mimetype='text/plain')
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        print(f"✅ アップロード大成功！ File ID: {file.get('id')}")
        print("Google ドライブのフォルダを確認してください。")

    except Exception as e:
        print(f"❌ エラー発生: {e}")

if __name__ == "__main__":
    test_oauth_upload()
