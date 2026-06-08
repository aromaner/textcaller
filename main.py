import os
import json
import base64
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.websockets import WebSocketDisconnect
from twilio.twiml.voice_response import VoiceResponse, Connect, Stream
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────
OPENAI_API_KEY     = os.getenv('OPENAI_API_KEY')
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN  = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE       = os.getenv('TWILIO_PHONE_NUMBER')
PORT               = int(os.getenv('PORT', 5050))
PUBLIC_URL         = os.getenv('PUBLIC_URL', '')

VOICE = 'alloy'
END_CALL_COMMANDS = {'end call', 'bye', 'goodbye', 'hang up', 'stop', 'end'}

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

sessions: dict = {}

app = FastAPI()

# ── Health check ───────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}

# ── SMS Webhook (/sms) ─────────────────────────────────────────────────────────
@app.api_route("/sms", methods=["GET", "POST"])
async def handle_sms(request: Request):
    form = await request.form()
    from_number = form.get('From', '').strip()
    body        = form.get('Body', '').strip()
    body_lower  = body.lower()

    resp = MessagingResponse()

    if from_number in sessions and sessions[from_number]['active']:
        session = sessions[from_number]

        if body_lower in END_CALL_COMMANDS:
            try:
                twilio_client.calls(session['call_sid']).update(status='completed')
            except Exception as e:
                print(f"Error hanging up: {e}")
            session['active'] = False
            resp.message("Call ended.")
            return HTMLResponse(content=str(resp), media_type="application/xml")

        openai_ws = session.get('openai_ws')
        if openai_ws and openai_ws.state.name == 'OPEN':
            asyncio.create_task(inject_text_as_speech(openai_ws, body))
        else:
            resp.message("(Could not relay message — call may have ended.)")

        return HTMLResponse(content=str(resp), media_type="application/xml")

    if body_lower.startswith('call '):
        voice_number = body[5:].strip().replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
        if voice_number.startswith('+'):
            pass  # already in E.164 format
        elif voice_number.startswith('1') and len(voice_number) == 11:
            voice_number = '+' + voice_number
        else:
            voice_number = '+1' + voice_number

        try:
            call = twilio_client.calls.create(
                to=voice_number,
                from_=TWILIO_PHONE,
                url=f'{PUBLIC_URL}/voice-connect?sms_from={from_number}',
                status_callback=f'{PUBLIC_URL}/call-status?sms_from={from_number}',
                status_callback_event=['completed', 'failed', 'no-answer', 'busy'],
                status_callback_method='POST'
            )
            sessions[from_number] = {
                'voice_number': voice_number,
                'call_sid': call.sid,
                'stream_sid': None,
                'openai_ws': None,
                'active': True
            }
            resp.message(f"Calling {voice_number}... I'll connect you when they pick up. Send any message to speak. Reply 'end call' to hang up.")
        except Exception as e:
            resp.message(f"Failed to place call: {e}")

        return HTMLResponse(content=str(resp), media_type="application/xml")

    resp.message("No active call. To start one, text: call +1XXXXXXXXXX")
    return HTMLResponse(content=str(resp), media_type="application/xml")


# ── Voice Connect Webhook (/voice-connect) ────────────────────────────────────
@app.api_route("/voice-connect", methods=["GET", "POST"])
async def voice_connect(request: Request):
    sms_from = request.query_params.get('sms_from', '')

    if sms_from:
        try:
            twilio_client.messages.create(
                body="They picked up! You're connected. Start texting to speak.",
                from_=TWILIO_PHONE,
                to=sms_from
            )
        except Exception as e:
            print(f"Error sending connected notification: {e}")

    response = VoiceResponse()
    connect = Connect()
    connect.stream(url=f'wss://{request.url.hostname}/media-stream?sms_from={sms_from}')
    response.append(connect)
    return HTMLResponse(content=str(response), media_type="application/xml")


# ── Call Status Webhook (/call-status) ────────────────────────────────────────
@app.api_route("/call-status", methods=["GET", "POST"])
async def call_status(request: Request):
    form = await request.form()
    sms_from    = request.query_params.get('sms_from', '')
    call_status = form.get('CallStatus', 'unknown')

    if sms_from and sms_from in sessions:
        sessions[sms_from]['active'] = False
        openai_ws = sessions[sms_from].get('openai_ws')
        if openai_ws:
            try:
                await openai_ws.close()
            except Exception:
                pass

        status_messages = {
            'completed': "Call ended.",
            'busy':      "The number was busy.",
            'no-answer': "No answer.",
            'failed':    "Call failed.",
        }
        msg = status_messages.get(call_status, f"Call ended ({call_status}).")
        try:
            twilio_client.messages.create(body=msg, from_=TWILIO_PHONE, to=sms_from)
        except Exception as e:
            print(f"Error sending status SMS: {e}")

    return JSONResponse({"ok": True})


# ── Media Stream WebSocket (/media-stream) ────────────────────────────────────
@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    await websocket.accept()
    sms_from = websocket.query_params.get('sms_from', '')
    print(f"Media stream connected for {sms_from}")

    async with websockets.connect(
        "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01",
        additional_headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "OpenAI-Beta": "realtime=v1"
        }
    ) as openai_ws:

        await initialize_session(openai_ws)

        if sms_from and sms_from in sessions:
            sessions[sms_from]['openai_ws'] = openai_ws

        stream_sid = None
        latest_media_timestamp = 0
        last_assistant_item = None
        mark_queue = []
        response_start_timestamp_twilio = None

        async def receive_from_twilio():
            nonlocal stream_sid, latest_media_timestamp
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    if data['event'] == 'media' and openai_ws.state.name == 'OPEN':
                        latest_media_timestamp = int(data['media']['timestamp'])
                        await openai_ws.send(json.dumps({
                            "type": "input_audio_buffer.append",
                            "audio": data['media']['payload']
                        }))
                    elif data['event'] == 'start':
                        stream_sid = data['start']['streamSid']
                        if sms_from and sms_from in sessions:
                            sessions[sms_from]['stream_sid'] = stream_sid
                        print(f"Stream started: {stream_sid}")
                        # Play intro message to the voice user
                        asyncio.create_task(inject_text_as_speech(
                            openai_ws,
                            "Hello. I am a voice assistant from TextCaller. "
                            "I have your caller on the line. They are communicating "
                            "via text message, which I will repeat to you."
                        ))
                    elif data['event'] == 'mark':
                        if mark_queue:
                            mark_queue.pop(0)
            except WebSocketDisconnect:
                print("Twilio disconnected.")
                if openai_ws.state.name == 'OPEN':
                    await openai_ws.close()

        async def send_to_twilio():
            nonlocal stream_sid, last_assistant_item, response_start_timestamp_twilio
            try:
                async for openai_message in openai_ws:
                    response = json.loads(openai_message)
                    event_type = response.get('type', '')

                    if event_type == 'response.audio.delta' and response.get('delta'):
                        audio_payload = base64.b64encode(
                            base64.b64decode(response['delta'])
                        ).decode('utf-8')
                        audio_delta = {
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {"payload": audio_payload}
                        }
                        await websocket.send_json(audio_delta)

                        if response_start_timestamp_twilio is None:
                            response_start_timestamp_twilio = latest_media_timestamp

                        if response.get('item_id'):
                            last_assistant_item = response['item_id']

                        await send_mark(websocket, stream_sid, mark_queue)

                    # Transcription from landline → SMS to text user
                    if event_type == 'conversation.item.input_audio_transcription.completed':
                        transcript = response.get('transcript', '').strip()
                        if transcript and sms_from:
                            try:
                                twilio_client.messages.create(
                                    body=f"📞 {transcript}",
                                    from_=TWILIO_PHONE,
                                    to=sms_from
                                )
                            except Exception as e:
                                print(f"Error sending transcript SMS: {e}")

                    if event_type == 'input_audio_buffer.speech_started':
                        if last_assistant_item and stream_sid:
                            await handle_speech_started_event(
                                websocket, openai_ws, stream_sid,
                                last_assistant_item, response_start_timestamp_twilio,
                                latest_media_timestamp, mark_queue
                            )
                            last_assistant_item = None
                            response_start_timestamp_twilio = None

            except Exception as e:
                print(f"Error in send_to_twilio: {e}")

        await asyncio.gather(receive_from_twilio(), send_to_twilio())


# ── Helpers ────────────────────────────────────────────────────────────────────
async def initialize_session(openai_ws):
    session_update = {
        "type": "session.update",
        "session": {
            "turn_detection": {"type": "server_vad"},
            "input_audio_format": "g711_ulaw",
            "output_audio_format": "g711_ulaw",
            "input_audio_transcription": {"model": "whisper-1"},
            "voice": VOICE,
            "instructions": (
                "You are a transparent audio relay. Your only job is to convert "
                "text input to speech and speech input to text. Do not add any "
                "words, commentary, or responses of your own. Simply relay audio "
                "as instructed."
            ),
            "modalities": ["text", "audio"],
            "temperature": 0.1,
        }
    }
    await openai_ws.send(json.dumps(session_update))


async def inject_text_as_speech(openai_ws, text: str):
    """Send a text message to OpenAI to be spoken aloud on the call."""
    message_event = {
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text}]
        }
    }
    await openai_ws.send(json.dumps(message_event))
    await openai_ws.send(json.dumps({"type": "response.create"}))


async def send_mark(websocket, stream_sid, mark_queue):
    if stream_sid:
        mark_event = {
            "event": "mark",
            "streamSid": stream_sid,
            "mark": {"name": "responsePart"}
        }
        await websocket.send_json(mark_event)
        mark_queue.append('responsePart')


async def handle_speech_started_event(
    websocket, openai_ws, stream_sid, last_assistant_item,
    response_start_timestamp_twilio, latest_media_timestamp, mark_queue
):
    if mark_queue and response_start_timestamp_twilio is not None:
        elapsed = latest_media_timestamp - response_start_timestamp_twilio
        if last_assistant_item:
            await openai_ws.send(json.dumps({
                "type": "conversation.item.truncate",
                "item_id": last_assistant_item,
                "content_index": 0,
                "audio_end_ms": elapsed
            }))
        await websocket.send_json({
            "event": "clear",
            "streamSid": stream_sid
        })
        mark_queue.clear()
