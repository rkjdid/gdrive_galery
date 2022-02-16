from dotenv import load_dotenv
load_dotenv()

import io
import os
import time
import urllib.parse
import requests
import base64

from loguru import logger

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

SCOPES = [
  'https://www.googleapis.com/auth/drive.metadata.readonly',
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/drive',
  'https://www.googleapis.com/auth/drive.file',
]
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_JSON', default='service_account-credentials.json')
ROOT_FOLDER = os.getenv('ROOT_FOLDER', default='')
SIZE_LIMIT = float(os.getenv('SIZE_LIMIT_KB', default=4096))

service_v2: Resource
service_v3: Resource
creds: Credentials

def init_services():
  global service_v2, service_v3, creds
  try:
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    creds.refresh(Request())
    if not creds or not creds.valid:
      logger.error("error with credentials")
      exit(1)
    service_v2 = build('drive', 'v2', credentials=creds)
    service_v3 = build('drive', 'v3', credentials=creds)
  except Exception as err:
    logger.error(err)
    exit(1)

def _stream(media):
  t0 = time.time()
  done = False
  buffer = io.BytesIO()
  downloader = MediaIoBaseDownload(buffer, media)
  progress = 0
  while done is False:
    _, done = downloader.next_chunk()
    buffer.seek(progress)
    yield buffer.read(downloader._progress - progress)
  logger.info("done in %.1fs" % (time.time() - t0))

def _streamFile():
  pass

def _download(url):
  headers = {}
  creds.apply(headers)
  return requests.get(url, headers=headers)

def listChildren(folder=ROOT_FOLDER, paginated=False, withThumbnail=False, pageSize=10, pageToken=None):
  global service_v2
  t0 = time.time()
  result = []
  sizeSkip = []
  paginated = paginated in ['true']
  withThumbnail = withThumbnail in ['true']
  pageSize = int(pageSize)
  while True:
    params = {'pageSize':pageSize}
    if pageToken:
      params['pageToken'] = pageToken
    params['q'] = "mimeType contains 'image/' and '%s' in parents" % folder
    params['fields'] = "nextPageToken, files(id, name, webContentLink, hasThumbnail, thumbnailLink, iconLink, description, size, fileExtension, mimeType)"
    results = service_v3.files().list(**params).execute()
    pageToken = results.get('nextPageToken')
    items = results.get('files', [])
    for item in items:
      fid = item['id']
      f = item
      f['fetchEndpoint'] = '/fetch/%s' % fid
      f['sizeKb'] = float(f.get('size', 0.)) / 1024.
      if not f['mimeType'].startswith("image/"):
        continue
      if f['sizeKb'] > SIZE_LIMIT:
        sizeSkip.append(f)
        continue
      tbField = 'thumbnailLink'
      if not f.get('hasThumbnail'):
        tbField = 'iconLink'
      if withThumbnail:
        tbResponse = _download(f[tbField])
        if tbResponse.ok:
          f['thumbnail'] = base64.b64encode(tbResponse.content).decode('utf-8')
          f['thumbnailMimeType'] = tbResponse.headers['Content-Type']
      f['thumbnailEndpoint'] = '/tunnel?url=%s' % urllib.parse.quote(f[tbField])
      logger.debug(f)
      result.append(f)
    if not pageToken or (paginated and pageSize <= len(result)):
      break
  logger.info("/list/%s in %.1fs, %d items, skipped %d (size limit: %.0f kb)" % (
    folder, time.time() - t0, len(result), len(sizeSkip), SIZE_LIMIT))
  return {'pageToken': pageToken, 'files': result}


from flask import Flask, request, jsonify, Response, abort
from flask_cors import CORS


init_services()
app = Flask(__name__)
CORS(app)

@app.route('/list', defaults={'folderId': ROOT_FOLDER})
@app.route('/list/', defaults={'folderId': ROOT_FOLDER})
@app.route('/list/<string:folderId>')
def route_list(folderId):
  try:
    result = listChildren(folderId, **request.args)
    return jsonify(result)
  except HttpError as err:
    return abort(err.status_code, err.reason)
  except Exception as err:
    logger.exception(err)
    return abort(500, err)

@app.route('/fetch/<string:fileId>')
def fetch(fileId):
  try:
    meta = service_v3.files().get(fileId=fileId, fields="mimeType, size").execute()
    logger.info("streaming %s: %s (%f kB)" % (fileId, meta.get('mimeType'), float(meta.get('size', 0.)) / 1024.))
    media = service_v3.files().get_media(fileId=fileId)
    return Response(_stream(media), mimetype=meta.get('mimeType'))
  except HttpError as err:
    abort(err.status_code, err.reason)
  except Exception as err:
    logger.exception(err)
    abort(500, err)

@app.route('/tunnel')
def tunnel():
  try:
    url = urllib.parse.unquote(request.args.get("url"))
    response = _download(url)
    if not response.ok:
      abort(response.status_code, response.reason)
    return Response(response.content, mimetype=response.headers.get("Content-Type"))
  except HttpError as err:
    abort(err.status_code, err.reason)
  except Exception as err:
    logger.exception(err)
    abort(500, err)

if __name__ == '__main__':
  app.run()

application = app
