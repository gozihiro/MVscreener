import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def debug_drive_upload():
    print("--- Google Drive 接続テスト開始 ---")
    
    # 1. 環境変数の読み込み確認
    gdrive_json = os.environ.get('GDRIVE_JSON')
    folder_id = os.environ.get('GDRIVE_FOLDER_ID', '').strip()
    
    if not gdrive_json:
        print("[ERROR] GDRIVE_JSON が設定されていません。")
        return
    if not folder_id:
        print("[ERROR] GDRIVE_FOLDER_ID が設定されていません。")
        return

    try:
        # 2. 認証情報のパース
        info = json.loads(gdrive_json)
        print(f"[OK] サービスアカウントのメールアドレス: {info.get('client_email')}")
        
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive']
        )
        service = build('drive', 'v3', credentials=creds)
        
        # 3. テストファイルの作成
        test_file = 'test_upload.txt'
        with open(test_file, 'w') as f:
            f.write("Google Drive Upload Test Content")
        print(f"[OK] テストファイル '{test_file}' をローカルに作成しました。")

        # 4. 親フォルダの存在・アクセス権確認 (ここが404の分岐点)
        print(f"フォルダID '{folder_id}' へのアクセスを試行中...")
        try:
            folder_meta = service.files().get(fileId=folder_id, fields='name, capabilities').execute()
            print(f"[OK] フォルダを確認しました。フォルダ名: {folder_meta.get('name')}")
            print(f"     書き込み権限: {folder_meta.get('capabilities', {}).get('canAddChildren')}")
        except Exception as e:
            print(f"[CRITICAL] フォルダにアクセスできません。IDが間違っているか、サービスアカウントが『編集者』として招待されていません。")
            print(f"詳細エラー: {e}")
            return

        # 5. アップロード実行
        file_metadata = {'name': test_file, 'parents': [folder_id]}
        media = MediaFileUpload(test_file, mimetype='text/plain', resumable=True)
        uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        
        print(f"[SUCCESS] アップロード完了！ File ID: {uploaded_file.get('id')}")
        print("Google Drive上でファイルが表示されているか確認してください。")

    except json.JSONDecodeError:
        print("[ERROR] GDRIVE_JSON が正しい形式のJSONではありません。")
    except Exception as e:
        print(f"[ERROR] 予期せぬエラーが発生しました: {e}")

if __name__ == "__main__":
    debug_drive_upload()
