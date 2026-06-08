import os
import json
import base64
import asyncio
import audioop
import httpx
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.websockets import WebSocketDisconnect
from twilio.twiml.voice_response import VoiceResponse, Connect
from twilio.twiml.messaging_response import MessagingResponse
from twilio.rest import Client
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────
OPENAI_API_KEY     = os.getenv('OPENAI_API_KEY')
DEEPGRAM_API_KEY   = os.getenv('DEEPGRAM_API_KEY')
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN  = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE       = os.getenv('TWILIO_PHONE_NUMBER')
PORT               = int(os.getenv('PORT', 5050))
PUBLIC_URL         = os.getenv('PUBLIC_URL', '')

# Word threshold: responses longer than this get summarized
SUMMARY_WORD_THRESHOLD = 20

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
    form        = await request.form()
    from_number = form.get('From', '').strip()
    body        = form.get('Body', '').strip()
    body_lower  = body.lower()

    resp = MessagingResponse()

    # ── Active call: relay text or end call ──
    if from_number in sessions and sessions[from_number].get('active'):
        session = sessions[from_number]

        if body_lower in END_CALL_COMMANDS:
            try:
                twilio_client.calls(session['call_sid']).update(status='completed')
            except Exception as e:
                print(f"Error hanging up: {e}")
            session['active'] = False
            resp.message("Call ended.")
            return HTMLResponse(content=str(resp), media_type="application/xml")

        tts_queue = session.get('tts_queue')
        if tts_queue is not None:
            await tts_queue.put(body)
        else:
            resp.message("(Could not relay message — call may have ended.)")

        return HTMLResponse(content=str(resp), media_type="application/xml")

    # ── Pending name: user is providing their name ──
    if from_number in sessions and sessions[from_number].get('awaiting_name'):
        session      = sessions[from_number]
        caller_name  = body.strip()
        voice_number = session['voice_number']
        session['caller_name']   = caller_name
        session['awaiting_name'] = False

        try:
            call = twilio_client.calls.create(
                to=voice_number,
                from_=TWILIO_PHONE,
                url=f'{PUBLIC_URL}/voice-connect?sms_from={quote(from_number)}&caller_name={quote(caller_name)}',
                status_callback=f'{PUBLIC_URL}/call-status?sms_from={quote(from_number)}',
                status_callback_event=['completed', 'failed', 'no-answer', 'busy'],
                status_callback_method='POST'
            )
            session['call_sid'] = call.sid
            session['active']   = True
            resp.message(f"Calling {voice_number}... I'll let you know when they pick up. Start texting to speak. Reply 'end call' to hang up.")
        except Exception as e:
            del sessions[from_number]
            resp.message(f"Failed to place call: {e}")

        return HTMLResponse(content=str(resp), media_type="application/xml")

    # ── New call request ──
    if body_lower.startswith('call '):
        voice_number = body[5:].strip().replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
        if voice_number.startswith('+'):
            pass
        elif voice_number.startswith('1') and len(voice_number) == 11:
            voice_number = '+' + voice_number
        else:
            voice_number = '+1' + voice_number

        sessions[from_number] = {
            'voice_number': voice_number,
            'call_sid':     None,
            'stream_sid':   None,
            'tts_queue':    None,
            'active':       False,
            'awaiting_name': True,
            'caller_name':  None
        }
        resp.message(
            "We will place your call in a moment. First, enter the name for your "
            "voice assistant with WhisperText to use when announcing your call."
        )
        return HTMLResponse(content=str(resp), media_type="application/xml")

    resp.message("No active call. To start one, text: call +1XXXXXXXXXX")
    return HTMLResponse(content=str(resp), media_type="application/xml")


# ── Voice Connect Webhook (/voice-connect) ────────────────────────────────────
@app.api_route("/voice-connect", methods=["GET", "POST"])
async def voice_connect(request: Request):
    sms_from    = request.query_params.get('sms_from', '')
    caller_name = request.query_params.get('caller_name', 'your caller')

    if sms_from:
        try:
            twilio_client.messages.create(
                body="They picked up! You're connected. Start texting to speak.",
                from_=TWILIO_PHONE,
                to=sms_from
            )
        except Exception as e:
            print(f"Error sending pickup notification: {e}")

    response = VoiceResponse()
    connect  = Connect()
    connect.stream(url=f'wss://{request.url.hostname}/media-stream?sms_from={quote(sms_from)}&caller_name={quote(caller_name)}')
    response.append(connect)
    return HTMLResponse(content=str(response), media_type="application/xml")


# ── Call Status Webhook (/call-status) ────────────────────────────────────────
@app.api_route("/call-status", methods=["GET", "POST"])
async def call_status(request: Request):
    form        = await request.form()
    sms_from    = request.query_params.get('sms_from', '')
    status      = form.get('CallStatus', 'unknown')

    if sms_from and sms_from in sessions:
        sessions[sms_from]['active'] = False
        tts_queue = sessions[sms_from].get('tts_queue')
        if tts_queue:
            await tts_queue.put(None)  # signal shutdown

        status_messages = {
            'completed': "Call ended.",
            'busy':      "The number was busy.",
            'no-answer': "No answer.",
            'failed':    "Call failed.",
        }
        msg = status_messages.get(status, f"Call ended ({status}).")
        try:
            twilio_client.messages.create(body=msg, from_=TWILIO_PHONE, to=sms_from)
        except Exception as e:
            print(f"Error sending status SMS: {e}")

    return JSONResponse({"ok": True})


# ── Media Stream WebSocket (/media-stream) ────────────────────────────────────
@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    await websocket.accept()
    sms_from    = websocket.query_params.get('sms_from', '')
    caller_name = websocket.query_params.get('caller_name', 'your caller')
    print(f"Media stream connected for {sms_from} (caller: {caller_name})")

    stream_sid = None
    tts_queue  = asyncio.Queue()

    if sms_from and sms_from in sessions:
        sessions[sms_from]['tts_queue'] = tts_queue

    # ── Deepgram STT connection ──
    dg_url = (
        "wss://api.deepgram.com/v1/listen"
        "?model=nova-3"
        "&encoding=mulaw"
        "&sample_rate=8000"
        "&channels=1"
        "&interim_results=false"
        "&utterance_end_ms=1200"
        "&vad_events=true"
        "&smart_format=true"
    )

    async with websockets.connect(
        dg_url,
        additional_headers={"Authorization": f"Token {DEEPGRAM_API_KEY}"}
    ) as dg_ws:

        # ── 1. Receive audio from Twilio → forward to Deepgram ──
        async def receive_from_twilio():
            nonlocal stream_sid
            try:
                async for message in websocket.iter_text():
                    data  = json.loads(message)
                    event = data.get('event')

                    if event == 'start':
                        stream_sid = data['start']['streamSid']
                        if sms_from and sms_from in sessions:
                            sessions[sms_from]['stream_sid'] = stream_sid
                        print(f"Stream started: {stream_sid}")
                        # Play intro after a short delay to let stream stabilize
                        asyncio.create_task(play_intro(websocket, stream_sid, caller_name))

                    elif event == 'media':
                        # Twilio sends mulaw 8kHz — forward directly to Deepgram
                        audio_bytes = base64.b64decode(data['media']['payload'])
                        await dg_ws.send(audio_bytes)

                    elif event == 'stop':
                        print("Twilio stream stopped.")
                        await dg_ws.send(json.dumps({"type": "CloseStream"}))
                        break

            except WebSocketDisconnect:
                print("Twilio disconnected.")
                try:
                    await dg_ws.send(json.dumps({"type": "CloseStream"}))
                except Exception:
                    pass

        # ── 2. Receive transcripts from Deepgram → summarize if needed → SMS ──
        async def receive_from_deepgram():
            try:
                async for message in dg_ws:
                    data = json.loads(message)
                    msg_type = data.get('type', '')

                    if msg_type == 'Results':
                        alt = data.get('channel', {}).get('alternatives', [{}])[0]
                        transcript = alt.get('transcript', '').strip()
                        is_final   = data.get('is_final', False)

                        if transcript and is_final:
                            word_count = len(transcript.split())

                            if word_count > SUMMARY_WORD_THRESHOLD:
                                # Summarize long response
                                summary = await summarize(transcript)
                                # Text the summary to the text user
                                if sms_from:
                                    try:
                                        twilio_client.messages.create(
                                            body=f"📞 {summary}",
                                            from_=TWILIO_PHONE,
                                            to=sms_from
                                        )
                                    except Exception as e:
                                        print(f"Error sending summary SMS: {e}")
                                # Read back confirmation to voice user
                                readback = f"Ok, I'll tell {caller_name}: {summary}"
                                await tts_queue.put(f"__READBACK__{readback}")
                            else:
                                # Short response — pass through as-is
                                if sms_from:
                                    try:
                                        twilio_client.messages.create(
                                            body=f"📞 {transcript}",
                                            from_=TWILIO_PHONE,
                                            to=sms_from
                                        )
                                    except Exception as e:
                                        print(f"Error sending transcript SMS: {e}")

            except Exception as e:
                print(f"Deepgram receive error: {e}")

        # ── 3. TTS queue: convert text → Deepgram Aura audio → Twilio ──
        async def process_tts_queue():
            while True:
                text = await tts_queue.get()
                if text is None:
                    break  # shutdown signal
                # Strip internal readback prefix if present
                if text.startswith("__READBACK__"):
                    text = text[len("__READBACK__"):]
                if stream_sid:
                    await speak_to_twilio(websocket, stream_sid, text)

        await asyncio.gather(
            receive_from_twilio(),
            receive_from_deepgram(),
            process_tts_queue()
        )


# ── Play intro to voice user ───────────────────────────────────────────────────
async def play_intro(websocket: WebSocket, stream_sid: str, caller_name: str):
    await asyncio.sleep(1)  # let stream settle
    intro = (
        f"Hello. I am a voice assistant with WhisperText. "
        f"I have {caller_name} on the line for you. "
        f"They are communicating via text messages, which I will repeat aloud to you. "
        f"I will do my best to record what you say and text it back to them."
    )
    await speak_to_twilio(websocket, stream_sid, intro)


# ── Deepgram Aura TTS → mulaw audio → Twilio ──────────────────────────────────
async def speak_to_twilio(websocket: WebSocket, stream_sid: str, text: str):
    """Convert text to speech via Deepgram Aura and stream to Twilio."""
    url = "https://api.deepgram.com/v1/speak?model=aura-asteria-en&encoding=linear16&sample_rate=8000"
    headers = {
        "Authorization": f"Token {DEEPGRAM_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {"text": text}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                audio_chunks = b""
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    audio_chunks += chunk

        # Convert linear16 PCM → mulaw 8kHz for Twilio
        mulaw_audio = audioop.lin2ulaw(audio_chunks, 2)

        # Send in 160-byte chunks (20ms frames at 8kHz mulaw)
        chunk_size = 160
        for i in range(0, len(mulaw_audio), chunk_size):
            chunk = mulaw_audio[i:i + chunk_size]
            payload = base64.b64encode(chunk).decode('utf-8')
            await websocket.send_json({
                "event": "media",
                "streamSid": stream_sid,
                "media": {"payload": payload}
            })
            await asyncio.sleep(0.02)  # pace at 20ms per frame

    except Exception as e:
        print(f"TTS error: {e}")


# ── GPT-4o-mini summarizer ─────────────────────────────────────────────────────
async def summarize(transcript: str) -> str:
    """Summarize a long transcript using GPT-4o-mini."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a relay assistant. Summarize the following spoken message "
                                "into one concise sentence suitable for an SMS. Keep it factual and brief. "
                                "Do not add any commentary or punctuation beyond the summary itself."
                            )
                        },
                        {"role": "user", "content": transcript}
                    ],
                    "max_tokens": 80,
                    "temperature": 0.3
                }
            )
            data = response.json()
            return data['choices'][0]['message']['content'].strip()
    except Exception as e:
        print(f"Summarize error: {e}")
        return transcript  # fallback: return original if summarization fails
