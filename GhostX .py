import asyncio
import logging
import json
import re
import shutil
import random
import hashlib
from datetime import datetime
from collections import deque
from telethon import TelegramClient, events, errors
from telethon.tl.types import (
    MessageMediaWebPage, MessageEntityTextUrl, MessageEntityUrl,
    MessageMediaPhoto, MessageMediaDocument, MessageMediaPoll,
    MessageMediaGeo, MessageMediaContact, MessageMediaVenue,
    MessageMediaGame, MessageMediaInvoice, MessageMediaGeoLive,
    MessageMediaDice, MessageMediaStory, InputMediaPoll, Poll,
    PollAnswer, InputReplyToMessage, MessageEntityMention, MessageEntityMentionName
)
from PIL import Image
import io
import os
from dotenv import load_dotenv
import unicodedata

# Configuration
load_dotenv()
API_ID = int(os.getenv('API_ID', 23617139))
API_HASH = os.getenv('API_HASH', '5bfc582b080fa09a1a2eaa6ee60fd5d4')
SESSION_FILE = "stealth_copy_bot_session"
MAPPINGS_FILE = "channel_mappings.json"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_QUEUE_SIZE = 100
MAX_MAPPING_HISTORY = 1000
INACTIVITY_THRESHOLD = 172800  # 6 hours in seconds
MAX_MESSAGE_LENGTH = 4096  # Telegram's max message length
QUEUE_INACTIVITY_THRESHOLD = 600  # 10 minutes in seconds
FAST_MODE = False
scramble_content_enabled = True
DEFAULT_DELAY_RANGE = [1, 5]
ANTI_FINGERPRINT_DELAY_RANGE = [2, 5]
NUM_WORKERS = 5
JITTER = 0.5  # ±0.5s jitter for delays
SILENT_MODE = False  # Disable verbose command outputs
NOTIFY_OWNER = True  # Enable owner notifications

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("stealth_copy_bot.log"), logging.StreamHandler()]
)
logger = logging.getLogger("StealthCopyBot")

# Initialize client
client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

# Data structures
channel_mappings = {}
message_queue = deque(maxlen=MAX_QUEUE_SIZE)
is_connected = False
pair_stats = {}
OWNER_ID = None
worker_tasks = []

# Known trap patterns
TRAP_VARIANTS = ["🔥 Black Dragon Entry 🔥", "EURUSD Buy @"]

# Helper Functions
def save_mappings():
    """Save channel mappings to a JSON file."""
    try:
        with open(MAPPINGS_FILE, "w") as f:
            json.dump(channel_mappings, f)
    except Exception as e:
        logger.error(f"Error saving mappings: {e}")

def load_mappings():
    """Load channel mappings from JSON, handling corrupted files."""
    global channel_mappings
    try:
        with open(MAPPINGS_FILE, "r") as f:
            channel_mappings = json.load(f)
        for user_id, pairs in channel_mappings.items():
            if user_id not in pair_stats:
                pair_stats[user_id] = {}
            for pair_name, mapping in pairs.items():
                mapping.setdefault('header_patterns', [])
                mapping.setdefault('footer_patterns', [])
                mapping.setdefault('remove_phrases', [])
                mapping.setdefault('remove_mentions', False)
                mapping.setdefault('trap_phrases', [])
                mapping.setdefault('trap_image_hashes', [])
                mapping.setdefault('delay_range', DEFAULT_DELAY_RANGE)
                mapping.setdefault('status', 'active')
                mapping.setdefault('last_activity', None)
                mapping.setdefault('stealth_mode', True)
                mapping.setdefault('content_scramble', False)
                mapping.setdefault('custom_header', '')
                mapping.setdefault('custom_footer', '')
                pair_stats[user_id][pair_name] = {
                    'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None
                }
    except FileNotFoundError:
        logger.info("No mappings file found. Starting fresh.")
    except json.JSONDecodeError as e:
        logger.error(f"Corrupted mappings file: {e}. Backing up.")
        shutil.move(MAPPINGS_FILE, MAPPINGS_FILE + ".bak")
        channel_mappings = {}
    except Exception as e:
        logger.error(f"Error loading mappings: {e}")

def compile_patterns(patterns):
    """Compile patterns into regex for efficient matching."""
    if not patterns:
        return None
    escaped = [re.escape(p.strip().lower()) for p in patterns if p.strip()]
    return re.compile('|'.join(escaped)) if escaped else None

def remove_patterns(text, patterns):
    """Remove specified patterns from text."""
    if not text or not patterns:
        return text
    compiled = compile_patterns(patterns)
    if compiled:
        lines = text.split('\n')
        filtered_lines = [line for line in lines if not compiled.match(line.strip().lower())]
        return '\n'.join(filtered_lines).strip()
    return text

def strip_invisible_characters(text):
    """Remove invisible Unicode characters to prevent fingerprinting."""
    if not text:
        return text
    return ''.join(c for c in text if unicodedata.category(c)[0] != 'C')

def log_fingerprint(text, timestamp, pair_name):
    """Log SHA256 fingerprint of text for reverse detection protection."""
    h = hashlib.sha256(text.encode()).hexdigest()[:12]
    logger.info(f"🧬 Fingerprint [{h}] for pair {pair_name} at {timestamp}")
    return h

def scramble_content_safe(text, entities=None):
    """Scramble content safely, preserving formatting entities."""
    if not text:
        return text, entities
    text = strip_invisible_characters(text)  # Strip invisible characters
    lines = [line for line in text.split('\n') if line.strip()]
    
    # Adjust entity offsets if needed
    new_entities = entities or []
    if len(lines) > 1:
        random.shuffle(lines)  # Shuffle non-essential lines
        if new_entities:
            # Recompute entity offsets after shuffling
            new_text = '\n'.join(lines)
            offset_map = {}
            original_pos = 0
            new_pos = 0
            for orig_line, new_line in zip(text.split('\n'), new_text.split('\n')):
                offset_map[original_pos] = new_pos
                original_pos += len(orig_line) + 1
                new_pos += len(new_line) + 1
            adjusted_entities = []
            for ent in new_entities:
                start = ent.offset
                end = ent.offset + ent.length
                new_start = offset_map.get(start, start)
                new_length = ent.length
                adjusted_entities.append(type(ent)(offset=new_start, length=new_length, **ent.__dict__))
            new_entities = adjusted_entities
        text = new_text.strip()
    else:
        text = '\n'.join(lines).strip()

    # Randomly add unique suffix or mutation
    if random.random() < 0.3:
        suffixes = ['😊', '👍', '🔥', '.', '..', '...']
        suffix = f" {random.choice(suffixes)}"
        text = text + suffix
        if new_entities:
            new_entities = [ent for ent in new_entities if ent.offset + ent.length <= len(text) - len(suffix)]
    if random.random() < 0.2:
        line_ending = '\n' if random.random() < 0.5 else '\r\n'
        text = text + line_ending.strip()
    if random.random() < 0.1:
        text = text + '\u200B'  # Controlled zero-width space
        if new_entities:
            new_entities = [ent for ent in new_entities if ent.offset + ent.length <= len(text) - 1]
    
    return text.strip(), new_entities

def calculate_image_hash(img_bytes):
    """Calculate MD5 hash of image bytes."""
    return hashlib.md5(img_bytes).hexdigest()

async def is_trap_image(media_bytes, mapping):
    """Check if media matches a trap image hash."""
    trap_hashes = mapping.get('trap_image_hashes', [])
    if not trap_hashes:
        return False
    return calculate_image_hash(media_bytes) in trap_hashes

def remove_mentions_entities(text, entities):
    """Remove @mentions and t.me links while preserving other formatting."""
    if not entities:
        return text, None

    new_text = ''
    new_entities = []
    last_offset = 0

    for ent in entities:
        entity_text = text[ent.offset:ent.offset + ent.length]

        is_mention = isinstance(ent, (MessageEntityMention, MessageEntityMentionName))
        is_tme_url = (
            isinstance(ent, (MessageEntityUrl, MessageEntityTextUrl)) and
            't.me/' in entity_text
        )

        if is_mention or is_tme_url:
            new_text += text[last_offset:ent.offset]
            last_offset = ent.offset + ent.length
        else:
            new_entities.append(ent)

    new_text += text[last_offset:]
    return new_text.strip(), new_entities

def clean_image(image):
    """Clean EXIF and modify image to break perceptual hashing."""
    try:
        image = image.convert('RGB')
        pixels = image.load()
        for x in range(0, eight.width, 20):
            for y in range(0, image.height, 20):
                r, g, b = pixels[x, y]
                pixels[x, y] = (r ^ 1, g ^ 1, b ^ 1)  # Flip 1 bit to disrupt pHash/dHash
        output = io.BytesIO()
        image.save(output, format='JPEG')
        return output.getvalue()
    except Exception as e:
        logger.error(f"Error cleaning image: {e}")
        return None

async def process_media(event, mapping):
    """Process media for stealth and trap checking."""
    try:
        media = event.message.media
        if isinstance(media, MessageMediaWebPage):
            return None  # Skip web page previews
        if isinstance(media, MessageMediaPhoto):
            photo = await client.download_media(event.message, bytes)
            if await is_trap_image(photo, mapping):
                reason = "Blocked image hash"
                await notify_trap(event, mapping, mapping['pair_name'], reason)
                return None
            image = Image.open(io.BytesIO(photo))
            processed_media = clean_image(image)
            if not processed_media:
                return None
            file_bytes = io.BytesIO(processed_media)
            file_bytes.name = f"stealth_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jpg"
            return file_bytes
        elif isinstance(media, MessageMediaDocument):
            file = await client.download_media(event.message, bytes)
            if await is_trap_image(file, mapping):
                reason = "Blocked image hash"
                await notify_trap(event, mapping, mapping['pair_name'], reason)
                return None
            file_bytes = io.BytesIO(file)
            file_bytes.name = f"stealth_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}{os.path.splitext(media.document.attributes[-1].file_name)[1]}"
            return file_bytes
        return media
    except Exception as e:
        logger.error(f"Error processing media: {e}")
        return None

async def notify_trap(event, mapping, pair_name, reason):
    """Notify owner of trapped content if enabled."""
    if NOTIFY_OWNER and OWNER_ID:
        msg_id = getattr(event.message, 'id', 'Unknown')
        await client.send_message(
            OWNER_ID,
            f"🛑 Trap detected in pair '{pair_name}' from '{mapping['source']}'.\n"
            f"📜 Reason: {reason}\n🆔 Source Message ID: {msg_id}"
        )

async def send_split_message(client, entity, message_text, reply_to=None, silent=False, entities=None):
    """Send long messages by splitting them into parts."""
    if len(message_text) <= MAX_MESSAGE_LENGTH:
        return await client.send_message(
            entity=entity,
            message=message_text,
            reply_to=reply_to,
            silent=silent,
            parse_mode='html',
            formatting_entities=entities or None
        )
    parts = [message_text[i:i + MAX_MESSAGE_LENGTH] for i in range(0, len(message_text), MAX_MESSAGE_LENGTH)]
    sent_messages = []
    for part in parts:
        sent_msg = await client.send_message(
            entity=entity,
            message=part,
            reply_to=reply_to if not sent_messages else None,
            silent=silent,
            parse_mode='html',
            formatting_entities=entities or None if not sent_messages else None
        )
        sent_messages.append(sent_msg)
        await asyncio.sleep(0.5)
    return sent_messages[0] if sent_messages else None

async def copy_message_with_retry(event, mapping, user_id, pair_name):
    """Copy message with retries and stealth features."""
    source_msg_id = event.message.id if hasattr(event.message, 'id') else "Unknown"
    for attempt in range(MAX_RETRIES):
        try:
            message_text = event.message.raw_text or ""
            text_lower = message_text.lower()
            original_entities = event.message.entities or []
            media = event.message.media
            if isinstance(media, MessageMediaWebPage):
                media = None  # Skip web page previews
            reply_to = await handle_reply_mapping(event, mapping)
            is_reply = reply_to is not None

            # Check for trap phrases
            compiled_traps = compile_patterns(mapping.get('trap_phrases', []))
            if compiled_traps and compiled_traps.search(text_lower):
                reason = "Trap phrase in text"
                await notify_trap(event, mapping, pair_name, reason)
                pair_stats[user_id][pair_name]['blocked'] += 1
                return True

            # Check for known trap variants
            if any(p.lower() in text_lower for p in TRAP_VARIANTS):
                reason = "Known trap pattern detected"
                await notify_trap(event, mapping, pair_name, reason)
                pair_stats[user_id][pair_name]['blocked'] += 1
                return True

            # Check for trap links
            if re.search(r"https?://(fxleaks|track|redirect|trk)\.", text_lower, re.IGNORECASE):
                reason = "Trap link detected"
                await notify_trap(event, mapping, pair_name, reason)
                pair_stats[user_id][pair_name]['blocked'] += 1
                return True

            # Text cleaning
            if message_text:
                message_text = remove_patterns(message_text, mapping.get('header_patterns', []))
                message_text = remove_patterns(message_text, mapping.get('footer_patterns', []))
                message_text, _ = remove_phrases(message_text, mapping.get('remove_phrases', []))
                if mapping.get('remove_mentions', False):
                    message_text, original_entities = remove_mentions_entities(message_text, original_entities)
                if is_reply and mapping.get('content_scramble', False):
                    message_text = re.sub(r'^>\s.*?\n', '', message_text, flags=re.MULTILINE)
                if scramble_content_enabled and mapping.get('content_scramble', False):
                    message_text, original_entities = scramble_content_safe(message_text, original_entities)
                message_text = apply_custom_header_footer(
                    message_text, mapping.get('custom_header', ''), mapping.get('custom_footer', '')
                )
                if message_text != event.message.raw_text:
                    original_entities = None

            # Log text fingerprint
            if message_text:
                log_fingerprint(message_text, datetime.now().isoformat(), pair_name)

            # Preserve formatting if text was modified
            if original_entities is None and message_text:
                message_text, original_entities = await client._parse_message_text(message_text, parse_mode='html')

            # Random delay with jitter for anti-time slot fingerprinting
            delay_range = REPLY_DELAY_RANGE if is_reply else mapping.get('delay_range', DEFAULT_DELAY_RANGE)
            if FAST_MODE:
                base_delay = random.uniform(*DEFAULT_DELAY_RANGE) + random.uniform(-0.2, 0.2)
                anti_fingerprint_delay = random.uniform(*ANTI_FINGERPRINT_DELAY_RANGE)
                total_delay = max(0, base_delay + anti_fingerprint_delay)
            else:
                base_delay = random.uniform(*DEFAULT_DELAY_RANGE)
                total_delay = base_delay
            if mapping.get('stealth_mode', True):
                await asyncio.sleep(total_delay)

            # Send message
            if processed_media := await process_media(event, mapping):
                sent_message = await client.send_message(
                    entity=int(mapping['destination']),
                    file=processed_media,
                    message=message_text,
                    reply_to=reply_to,
                    silent=event.message.silent,
                    parse_mode='html',
                    formatting_entities=original_entities or None
                )
            else:
                if not message_text.strip():
                    reason = "Empty message after filtering"
                    await notify_trap(event, mapping, pair_name, reason)
                    pair_stats[user_id][pair_name]['blocked'] += 1
                    return True
                sent_message = await send_split_message(
                    client,
                    int(mapping['destination']),
                    message_text,
                    reply_to=reply_to,
                    silent=event.message.silent,
                    entities=original_entities
                )

            await store_message_mapping(event, mapping, sent_message)
            pair_stats[user_id][pair_name]['forwarded'] += 1
            pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
            if not mapping.get('stealth_mode', True):
                logger.info(f"Copied message from {mapping['source']} to {mapping['destination']}")
            return True

        except errors.FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"Flood wait error, sleeping for {wait_time}s for pair '{pair_name}'")
            await asyncio.sleep(wait_time)
        except errors.ChatWriteForbiddenError as e:
            logger.warning(f"Bot forbidden to write in {mapping['destination']}. Pausing pair '{pair_name}'.")
            mapping['status'] = 'paused'
            save_mappings()
            if NOTIFY_OWNER and OWNER_ID:
                await client.send_message(OWNER_ID, f"⚠️ Paused pair '{pair_name}' due to write permission error.")
            return False
        except errors.ChannelInvalidError as e:
            logger.warning(f"Invalid channel {mapping['destination']}. Pausing pair '{pair_name}'.")
            mapping['status'] = 'paused'
            save_mappings()
            if NOTIFY_OWNER and OWNER_ID:
                await client.send_message(OWNER_ID, f"⚠️ Paused pair '{pair_name}' due to invalid channel.")
            return False
        except Exception as e:
            logger.error(f"Error copying message for pair '{pair_name}': {e}")
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                await asyncio.sleep(wait_time)
            else:
                if NOTIFY_OWNER and OWNER_ID:
                    await client.send_message(OWNER_ID, f"❌ Failed to copy message for pair '{pair_name}' after {MAX_RETRIES} attempts.")
                return False

async def edit_copied_message(event, mapping, user_id, pair_name):
    """Edit a copied message when the source is edited."""
    try:
        if not hasattr(client, 'forwarded_messages'):
            client.forwarded_messages = {}
        mapping_key = f"{mapping['source']}:{event.message.id}"
        if mapping_key not in client.forwarded_messages:
            return

        forwarded_msg_id = client.forwarded_messages[mapping_key]
        forwarded_msg = await client.get_messages(int(mapping['destination']), ids=forwarded_msg_id)
        if not forwarded_msg:
            del client.forwarded_messages[mapping_key]
            return

        message_text = event.message.raw_text or ""
        text_lower = message_text.lower()
        original_entities = event.message.entities or []
        media = event.message.media
        if isinstance(media, MessageMediaWebPage):
            media = None  # Skip web page previews
        reply_to = await handle_reply_mapping(event, mapping)
        is_reply = reply_to is not None

        # Check traps
        compiled_traps = compile_patterns(mapping.get('trap_phrases', []))
        if compiled_traps and compiled_traps.search(text_lower):
            reason = "Trap phrase in edited text"
            await notify_trap(event, mapping, pair_name, reason)
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            pair_stats[user_id][pair_name]['blocked'] += 1
            pair_stats[user_id][pair_name]['deleted'] += 1
            return

        # Check for known trap variants
        if any(p.lower() in text_lower for p in TRAP_VARIANTS):
            reason = "Known trap pattern in edited text"
            await notify_trap(event, mapping, pair_name, reason)
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            pair_stats[user_id][pair_name]['blocked'] += 1
            pair_stats[user_id][pair_name]['deleted'] += 1
            return

        # Check for trap links
        if re.search(r"https?://(fxleaks|track|redirect|trk)\.", text_lower, re.IGNORECASE):
            reason = "Trap link in edited text"
            await notify_trap(event, mapping, pair_name, reason)
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            pair_stats[user_id][pair_name]['blocked'] += 1
            pair_stats[user_id][pair_name]['deleted'] += 1
            return

        # Text cleaning
        if message_text:
            message_text = remove_patterns(message_text, mapping.get('header_patterns', []))
            message_text = remove_patterns(message_text, mapping.get('footer_patterns', []))
            message_text, _ = remove_phrases(message_text, mapping.get('remove_phrases', []))
            if mapping.get('remove_mentions', False):
                message_text, original_entities = remove_mentions_entities(message_text, original_entities)
            if is_reply and mapping.get('content_scramble', False):
                message_text = re.sub(r'^>\s.*?\n', '', message_text, flags=re.MULTILINE)
            if scramble_content_enabled and mapping.get('content_scramble', False):
                message_text, original_entities = scramble_content_safe(message_text, original_entities)
            message_text = apply_custom_header_footer(
                message_text, mapping.get('custom_header', ''), mapping.get('custom_footer', '')
            )
            if message_text != event.message.raw_text:
                original_entities = None

        # Log text fingerprint
        if message_text:
            log_fingerprint(message_text, datetime.now().isoformat(), pair_name)

        # Process media
        processed_media = await process_media(event, mapping)
        if processed_media is None and isinstance(media, (MessageMediaPhoto, MessageMediaDocument)):
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            pair_stats[user_id][pair_name]['blocked'] += 1
            pair_stats[user_id][pair_name]['deleted'] += 1
            return

        if not message_text.strip() and not processed_media:
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            reason = "Empty message after filtering"
            await notify_trap(event, mapping, pair_name, reason)
            pair_stats[user_id][pair_name]['blocked'] += 1
            pair_stats[user_id][pair_name]['deleted'] += 1
            return

        if isinstance(media, MessageMediaPoll):
            await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
            del client.forwarded_messages[mapping_key]
            await copy_message_with_retry(event, mapping, user_id, pair_name)
            return

        # Preserve formatting if text was modified
        if original_entities is None and message_text:
            message_text, original_entities = await client._parse_message_text(message_text, parse_mode='html')

        await client.edit_message(
            entity=int(mapping['destination']),
            message=forwarded_msg_id,
            text=message_text,
            file=processed_media if processed_media else None,
            parse_mode='html',
            formatting_entities=original_entities or None
        )
        pair_stats[user_id][pair_name]['edited'] += 1
        pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
        if not mapping.get('stealth_mode', True):
            logger.info(f"Edited copied message {forwarded_msg_id} in {mapping['destination']}")

    except Exception as e:
        logger.error(f"Error editing message for pair '{pair_name}': {e}")

async def delete_copied_message(event, mapping, user_id, pair_name):
    """Delete a copied message when the source is deleted."""
    try:
        if not hasattr(client, 'forwarded_messages'):
            client.forwarded_messages = {}
        mapping_key = f"{mapping['source']}:{event.message.id}"
        if mapping_key not in client.forwarded_messages:
            return
        forwarded_msg_id = client.forwarded_messages[mapping_key]
        await client.delete_messages(int(mapping['destination']), [forwarded_msg_id])
        pair_stats[user_id][pair_name]['deleted'] += 1
        pair_stats[user_id][pair_name]['last_activity'] = datetime.now().isoformat()
        del client.forwarded_messages[mapping_key]
        if not mapping.get('stealth_mode', True):
            logger.info(f"Deleted copied message {forwarded_msg_id} in {mapping['destination']}")
    except Exception as e:
        logger.error(f"Error deleting copied message for pair '{pair_name}': {e}")

async def handle_reply_mapping(event, mapping):
    """Map replies from source to destination messages."""
    if not hasattr(event.message, 'reply_to') or not event.message.reply_to:
        return None
    try:
        source_reply_id = event.message.reply_to.reply_to_msg_id
        mapping_key = f"{mapping['source']}:{source_reply_id}"
        if hasattr(client, 'forwarded_messages') and mapping_key in client.forwarded_messages:
            dest_reply_id = client.forwarded_messages[mapping_key]
            return dest_reply_id if dest_reply_id else None
        return None
    except Exception as e:
        logger.error(f"Error handling reply mapping for pair '{mapping.get('pair_name', 'unknown')}': {e}")
        return None

async def store_message_mapping(event, mapping, sent_message):
    """Store mapping of source to destination message IDs."""
    try:
        if not hasattr(event.message, 'id'):
            return
        if not hasattr(client, 'forwarded_messages'):
            client.forwarded_messages = {}
        if len(client.forwarded_messages) >= MAX_MAPPING_HISTORY:
            oldest_key = next(iter(client.forwarded_messages))
            client.forwarded_messages.pop(oldest_key)
        mapping_key = f"{mapping['source']}:{event.message.id}"
        client.forwarded_messages[mapping_key] = sent_message.id
    except Exception as e:
        logger.error(f"Error storing message mapping for pair '{mapping.get('pair_name', 'unknown')}': {e}")

def remove_phrases(text, phrases):
    """Remove specific phrases from text."""
    if not text or not phrases:
        return text, False
    found = False
    for phrase in phrases:
        if phrase.lower() in text.lower():
            text = text.replace(phrase, '')
            found = True
    return re.sub(r'\s+', ' ', text).strip(), found

def apply_custom_header_footer(text, header, footer):
    """Apply custom header and footer to message text."""
    if not text:
        return text
    result = text
    if header:
        result = header + '\n' + result
    if footer:
        result = result + '\n' + footer
    return result.strip()

# Event Handlers
@client.on(events.NewMessage(pattern='(?i)^/sx$'))
async def start(event):
    """Handle /sx command."""
    global OWNER_ID
    OWNER_ID = event.sender_id
    if not SILENT_MODE:
        await event.reply("✅ StealthCopyBot Running!\nUse `/commands` for options.")

@client.on(events.NewMessage(pattern='(?i)^/commands$'))
async def list_commands(event):
    """List all available commands."""
    if SILENT_MODE:
        return
    commands = """
📋 StealthCopyBot Commands

**Setup & Management**
- `/setpair <name> <source> <dest> [yes|no]` - Add pair (yes/no for mentions)
- `/listpairs` - Show all pairs
- `/pausepair <name>` - Pause a pair
- `/resumepair <name>` - Resume a pair
- `/pauseall` - Pause all pairs
- `/resumeall` - Resume all pairs
- `/clearpairs` - Remove all pairs
- `/setdelay <name> <min> <max>` - Set random delay range
- `/setfastmode <on|off>` - Toggle fast mode
- `/status <name>` - Check pair status
- `/report` - View pair stats
- `/monitor` - Detailed pair monitor
- `/enablestealth <name>` - Enable stealth mode
- `/disablestealth <name>` - Disable stealth mode
- `/enablescramble <name>` - Enable content scrambling
- `/disablescramble <name>` - Disable content scrambling

**🧽 Text Cleaning**
- `/addheader <pair> <pattern>` - Add header to remove
- `/removeheader <pair> <pattern>` - Remove header
- `/addfooter <pair> <pattern>` - Add footer to remove
- `/removefooter <pair> <pattern>` - Remove footer
- `/addremoveword <pair> <phrase>` - Add phrase to remove
- `/removeword <pair> <phrase>` - Remove phrase
- `/enablementionremoval <pair>` - Enable mention removal
- `/disablementionremoval <pair>` - Disable mention removal
- `/showfilters <pair>` - Show text filters
- `/setcustomheader <pair> <text>` - Set custom header
- `/setcustomfooter <pair> <text>` - Set custom footer
- `/clearcustomheaderfooter <pair>` - Clear custom text

**🔐 Trap Filters**
- `/addtrapword <pair> <word>` - Add trap phrase
- `/removetrapword <pair> <word>` - Remove trap phrase
- `/addtrapimage <pair>` - Add trap image (reply to image)
- `/removetrapimage <pair>` - Remove trap image (reply to image)
- `/showtraps <pair>` - Show trap filters
"""
    await event.reply(commands)

@client.on(events.NewMessage(pattern=r'/setfastmode (on|off)'))
async def toggle_fast_mode(event):
    """Toggle FAST_MODE to adjust delays and worker count."""
    global FAST_MODE, DEFAULT_DELAY_RANGE, ANTI_FINGERPRINT_DELAY_RANGE, NUM_WORKERS, worker_tasks
    arg = event.pattern_match.group(1).lower()
    if arg == "on":
        FAST_MODE = True
        DEFAULT_DELAY_RANGE = [0.5, 1.5]
        ANTI_FINGERPRINT_DELAY_RANGE = [0.5, 1.5]
        old_num_workers = NUM_WORKERS
        NUM_WORKERS = 10
        await event.reply("🚀 FAST MODE ENABLED\nBot will forward quickly with stealth.")
    else:
        FAST_MODE = False
        DEFAULT_DELAY_RANGE = [1, 5]
        ANTI_FINGERPRINT_DELAY_RANGE = [2, 5]
        old_num_workers = NUM_WORKERS
        NUM_WORKERS = 5
        await event.reply("🐢 NORMAL MODE ENABLED\nStandard delay and throughput restored.")

    # Adjust worker tasks
    if old_num_workers != NUM_WORKERS:
        # Cancel existing worker tasks
        for task in worker_tasks:
            task.cancel()
        worker_tasks.clear()
        # Start new worker tasks
        for _ in range(NUM_WORKERS):
            task = asyncio.create_task(queue_worker())
            worker_tasks.append(task)
        logger.info(f"Adjusted to {NUM_WORKERS} queue workers.")

@client.on(events.NewMessage(pattern=r'/setpair (\S+) (\S+) (\S+)(?: (yes|no))?'))
async def set_pair(event):
    """Add a new forwarding pair."""
    pair_name, source, destination, remove_mentions = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    remove_mentions = remove_mentions == "yes"
    if user_id not in channel_mappings:
        channel_mappings[user_id] = {}
    if user_id not in pair_stats:
        pair_stats[user_id] = {}
    channel_mappings[user_id][pair_name] = {
        'source': source.strip(),
        'destination': destination.strip(),
        'status': 'active',
        'remove_mentions': remove_mentions,
        'header_patterns': [],
        'footer_patterns': [],
        'remove_phrases': [],
        'trap_phrases': [],
        'trap_image_hashes': [],
        'delay_range': DEFAULT_DELAY_RANGE,
        'stealth_mode': True,
        'content_scramble': False,
        'custom_header': '',
        'custom_footer': '',
        'last_activity': None
    }
    pair_stats[user_id][pair_name] = {'forwarded': 0, 'edited': 0, 'deleted': 0, 'blocked': 0, 'queued': 0, 'last_activity': None}
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"✅ Pair '{pair_name}' added: {source} ➡️ {destination}\nMentions removal: {'✅' if remove_mentions else '❌'}")

@client.on(events.NewMessage(pattern=r'/enablestealth (\S+)'))
async def enable_stealth(event):
    """Enable stealth mode for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['stealth_mode'] = True
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"✅ Stealth mode enabled for '{pair_name}'.")

@client.on(events.NewMessage(pattern=r'/disablestealth (\S+)'))
async def disable_stealth(event):
    """Disable stealth mode for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['stealth_mode'] = False
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"❌ Stealth mode disabled for '{pair_name}'.")

@client.on(events.NewMessage(pattern=r'/enablescramble (\S+)'))
async def enable_scramble(event):
    """Enable content scrambling for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['content_scramble'] = True
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"✅ Content scrambling enabled for '{pair_name}'.")

@client.on(events.NewMessage(pattern=r'/disablescramble (\S+)'))
async def disable_scramble(event):
    """Disable content scrambling for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['content_scramble'] = False
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"❌ Content scrambling disabled for '{pair_name}'.")

@client.on(events.NewMessage(pattern=r'/addheader (\S+) (.+)'))
async def add_header(event):
    """Add header pattern to remove."""
    pair_name, pattern = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    pattern = pattern.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if pattern not in channel_mappings[user_id][pair_name]['header_patterns']:
        channel_mappings[user_id][pair_name]['header_patterns'].append(pattern)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"📑 Added header pattern for '{pair_name}': {pattern}")

@client.on(events.NewMessage(pattern=r'/removeheader (\S+) (.+)'))
async def remove_header(event):
    """Remove header pattern."""
    pair_name, pattern = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    pattern = pattern.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if pattern in channel_mappings[user_id][pair_name]['header_patterns']:
        channel_mappings[user_id][pair_name]['header_patterns'].remove(pattern)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"🗑️ Removed header pattern from '{pair_name}': {pattern}")
    else:
        await event.reply("❌ Header pattern not found.")

@client.on(events.NewMessage(pattern=r'/addfooter (\S+) (.+)'))
async def add_footer(event):
    """Add footer pattern to remove."""
    pair_name, pattern = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    pattern = pattern.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if pattern not in channel_mappings[user_id][pair_name]['footer_patterns']:
        channel_mappings[user_id][pair_name]['footer_patterns'].append(pattern)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"📑 Added footer pattern for '{pair_name}': {pattern}")
    else:
        await event.reply("⚠️ Footer pattern already exists.")

@client.on(events.NewMessage(pattern=r'/removefooter (\S+) (.+)'))
async def remove_footer(event):
    """Remove footer pattern."""
    pair_name, pattern = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    pattern = pattern.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if pattern in channel_mappings[user_id][pair_name]['footer_patterns']:
        channel_mappings[user_id][pair_name]['footer_patterns'].remove(pattern)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"🗑️ Removed footer pattern from '{pair_name}': {pattern}")
    else:
        await event.reply("❌ Footer pattern not found.")

@client.on(events.NewMessage(pattern=r'/addremoveword (\S+) (.+)'))
async def add_remove_word(event):
    """Add phrase to remove."""
    pair_name, phrase = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    phrase = phrase.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if phrase not in channel_mappings[user_id][pair_name]['remove_phrases']:
        channel_mappings[user_id][pair_name]['remove_phrases'].append(phrase)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"🧹 Added phrase to remove for '{pair_name}': {phrase}")

@client.on(events.NewMessage(pattern=r'/removeword (\S+) (.+)'))
async def remove_word(event):
    """Remove phrase from removal list."""
    pair_name, phrase = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    phrase = phrase.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if phrase in channel_mappings[user_id][pair_name]['remove_phrases']:
        channel_mappings[user_id][pair_name]['remove_phrases'].remove(phrase)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"🗑️ Removed phrase from '{pair_name}': {phrase}")
    else:
        await event.reply("❌ Phrase not found.")

@client.on(events.NewMessage(pattern=r'/enablementionremoval (\S+)'))
async def enable_mention_removal(event):
    """Enable mention removal for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['remove_mentions'] = True
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"✅ Mention removal enabled for '{pair_name}'.")

@client.on(events.NewMessage(pattern=r'/disablementionremoval (\S+)'))
async def disable_mention_removal(event):
    """Disable mention removal for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['remove_mentions'] = False
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"❌ Mention removal disabled for '{pair_name}'.")

@client.on(events.NewMessage(pattern=r'/showfilters (\S+)'))
async def show_filters(event):
    """Show all text filters for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    mapping = channel_mappings[user_id][pair_name]
    filters = (
        f"📋 Filters for '{pair_name}':\n"
        f"🟦 Headers: {', '.join(mapping['header_patterns']) if mapping['header_patterns'] else 'None'}\n"
        f"🟦 Footers: {', '.join(mapping['footer_patterns']) if mapping['footer_patterns'] else 'None'}\n"
        f"🟦 Phrases: {', '.join(mapping['remove_phrases']) if mapping['remove_phrases'] else 'None'}\n"
        f"🟦 Mentions: {'Enabled' if mapping['remove_mentions'] else 'Disabled'}\n"
        f"🟦 Custom Header: {mapping.get('custom_header', 'None')}\n"
        f"🟦 Custom Footer: {mapping.get('custom_footer', 'None')}"
    )
    if not SILENT_MODE:
        await event.reply(filters)

@client.on(events.NewMessage(pattern=r'/addtrapword (\S+) (.+)'))
async def add_trap_word(event):
    """Add trap phrase."""
    pair_name, word = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    word = word.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if word not in channel_mappings[user_id][pair_name]['trap_phrases']:
        channel_mappings[user_id][pair_name]['trap_phrases'].append(word)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"🛑 Added block phrase for '{pair_name}': {word}")

@client.on(events.NewMessage(pattern=r'/removetrapword (\S+) (.+)'))
async def remove_trap_word(event):
    """Remove trap phrase."""
    pair_name, word = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    word = word.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if word in channel_mappings[user_id][pair_name]['trap_phrases']:
        channel_mappings[user_id][pair_name]['trap_phrases'].remove(word)
        save_mappings()
        if not SILENT_MODE:
            await event.reply(f"🗑️ Removed block phrase from '{pair_name}': {word}")
    else:
        await event.reply("❌ Block phrase not found.")

@client.on(events.NewMessage(pattern=r'/addtrapimage (\S+)'))
async def add_trap_image(event):
    """Add trap image hash via reply."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if not event.message.reply_to:
        await event.reply("📷 Please reply to an image or document.")
        return
    replied_msg = await event.get_reply_message()
    if not isinstance(replied_msg.media, (MessageMediaPhoto, MessageMediaDocument)):
        await event.reply("❌ No image or document in replied message.")
        return
    try:
        media = await client.download_media(replied_msg, bytes)
        image_hash = calculate_image_hash(media)
        if image_hash not in channel_mappings[user_id][pair_name]['trap_image_hashes']:
            channel_mappings[user_id][pair_name]['trap_image_hashes'].append(image_hash)
            save_mappings()
            if not SILENT_MODE:
                await event.reply(f"🛑 Added trap image hash for '{pair_name}': {image_hash}")
        else:
            await event.reply(f"⚠️ Trap image hash already exists in '{pair_name}'.")
    except Exception as e:
        await event.reply(f"❌ Error adding trap image: {str(e)}")
        logger.error(f"Error adding trap image for '{pair_name}': {e}")

@client.on(events.NewMessage(pattern=r'/removetrapimage (\S+)'))
async def remove_trap_image(event):
    """Remove trap image hash via reply."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if not event.message.reply_to:
        await event.reply("📷 Please reply to an image or document.")
        return
    replied_msg = await event.get_reply_message()
    if not isinstance(replied_msg.media, (MessageMediaPhoto, MessageMediaDocument)):
        await event.reply("❌ No image or document in replied message.")
        return
    try:
        media = await client.download_media(replied_msg, bytes)
        image_hash = calculate_image_hash(media)
        if image_hash in channel_mappings[user_id][pair_name]['trap_image_hashes']:
            channel_mappings[user_id][pair_name]['trap_image_hashes'].remove(image_hash)
            save_mappings()
            if not SILENT_MODE:
                await event.reply(f"🗑️ Removed trap image hash from '{pair_name}': {image_hash}")
        else:
            await event.reply(f"❌ Trap image hash not found in '{pair_name}'.")
    except Exception as e:
        await event.reply(f"❌ Error removing trap image: {str(e)}")
        logger.error(f"Error removing trap image for '{pair_name}': {e}")

@client.on(events.NewMessage(pattern=r'/showtraps (\S+)'))
async def show_traps(event):
    """Show all block filters."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    mapping = channel_mappings[user_id][pair_name]
    traps = (
        f"🛑 Block filters for '{pair_name}':\n"
        f"📜 Phrases: {', '.join(mapping['trap_phrases']) if mapping['trap_phrases'] else 'None'}\n"
        f"📷 Image Hashes: {', '.join(mapping['trap_image_hashes']) if mapping['trap_image_hashes'] else 'None'}\n"
    )
    if not SILENT_MODE:
        await event.reply(traps)

@client.on(events.NewMessage(pattern=r'/pausepair (\S+)'))
async def pause_pair(event):
    """Pause a forwarding pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['status'] = 'paused'
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"⏸️ Pair '{pair_name}' paused.")

@client.on(events.NewMessage(pattern=r'/resumepair (\S+)'))
async def resume_pair(event):
    """Resume a forwarding pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['status'] = 'active'
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"▶️ Pair '{pair_name}' resumed.")

@client.on(events.NewMessage(pattern='(?i)^/pauseall$'))
async def pause_all(event):
    """Pause all pairs for the user."""
    user_id = str(event.sender_id)
    if user_id not in channel_mappings:
        await event.reply("❌ No pairs found.")
        return
    for pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['status'] = 'paused'
    save_mappings()
    if not SILENT_MODE:
        await event.reply("⏸️ All pairs paused.")

@client.on(events.NewMessage(pattern='(?i)^/resumeall$'))
async def resume_all(event):
    """Resume all pairs for the user."""
    user_id = str(event.sender_id)
    if user_id not in channel_mappings:
        await event.reply("❌ No pairs found.")
        return
    for pair_name in channel_mappings[user_id]:
        channel_mappings[user_id][pair_name]['status'] = 'active'
    save_mappings()
    if not SILENT_MODE:
        await event.reply("▶️ All pairs resumed.")

@client.on(events.NewMessage(pattern=r'/setdelay (\S+) (\d*\.?\d+) (\d*\.?\d+)'))
async def set_delay(event):
    """Set random delay range for a pair."""
    pair_name, min_delay, max_delay = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    min_delay, max_delay = float(min_delay), float(max_delay)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    if min_delay < 0 or max_delay < min_delay:
        await event.reply("❌ Invalid delay range.")
        return
    channel_mappings[user_id][pair_name]['delay_range'] = [min_delay, max_delay]
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"⏱️ Set delay range for '{pair_name}': {min_delay}s - {max_delay}s")

@client.on(events.NewMessage(pattern=r'/status (\S+)'))
async def status_pair(event):
    """Check status of a specific pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    mapping = channel_mappings[user_id][pair_name]
    stats = pair_stats.get(user_id, {}).get(pair_name, {})
    status_msg = (
        f"🛠 Pair '{pair_name}' Status\n"
        f"📡 Route: {mapping['source']} ➡️ {mapping['destination']}\n"
        f"✅ Status: {mapping['status'].capitalize()}\n"
        f"📈 Stats: Fwd: {stats.get('forwarded', 0)} | Edt: {stats.get('edited', 0)} | "
        f"Del: {stats.get('deleted', 0)} | Blk: {stats.get('blocked', 0)} | Que: {stats.get('queued', 0)}\n"
        f"⏰ Last Activity: {stats.get('last_activity', 'N/A')}\n"
        f"🕵️ Stealth: {'Enabled' if mapping['stealth_mode'] else 'Disabled'}\n"
        f"🔄 Scramble: {'Enabled' if mapping['content_scramble'] else 'Disabled'}"
    )
    if not SILENT_MODE:
        await event.reply(status_msg)

@client.on(events.NewMessage(pattern='(?i)^/report$'))
async def report(event):
    """Show summary report of all pairs."""
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply("❌ No pairs found.")
        return
    report = ["📊 StealthCopyBot Report"]
    for pair_name, data in channel_mappings[user_id].items():
        stats = pair_stats.get(user_id, {}).get(pair_name, {})
        report.append(
            f"📌 {pair_name}: {data['source']} ➡️ {data['destination']} [{data['status'].capitalize()}]\n"
            f"   📈 Fwd: {stats.get('forwarded', 0)} | Edt: {stats.get('edited', 0)} | "
            f"Del: {stats.get('deleted', 0)} | Blk: {stats.get('blocked', 0)}"
        )
    report.append(f"📥 Queue Size: {len(message_queue)}/{MAX_QUEUE_SIZE}")
    if not SILENT_MODE:
        await send_split_message_event(event, "\n".join(report))

@client.on(events.NewMessage(pattern='(?i)^/monitor$'))
async def monitor_pairs(event):
    """Detailed monitoring of pairs."""
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    report = ["📊 Detailed Monitor"]
    for pair_name, data in channel_mappings[user_id].items():
        stats = pair_stats.get(user_id, {}).get(pair_name, {})
        last_activity = stats.get('last_activity', 'N/A')
        if len(last_activity) > 20:
            last_activity = last_activity[:17] + "..."
        report.append(
            f"📌 {pair_name}\n"
            f"   ➡️ Route: {data['source']} ➡️ {data['destination']}\n"
            f"   ✅ Status: {data['status'].capitalize()}\n"
            f"   📈 Stats: Fwd: {stats.get('forwarded', 0)} | Edt: {stats.get('edited', 0)} | "
            f"Del: {stats.get('deleted', 0)} | Blk: {stats.get('blocked', 0)} | Que: {stats.get('queued', 0)}\n"
            f"   ⏰ Last: {last_activity}\n"
            f"   🧹 Filters: Headers({len(data['header_patterns'])}), Footers({len(data['footer_patterns'])}), "
            f"Phrases({len(data['remove_phrases'])}), Mentions({'✅' if data['remove_mentions'] else '❌'})\n"
            f"   🕵️ Stealth: {'✅' if data['stealth_mode'] else '❌'}\n"
            f"   🔄 Scramble: {'✅' if data['content_scramble'] else '❌'}\n"
            f"   ⏱️ Delay: {data['delay_range'][0]}s - {data['delay_range'][1]}s"
        )
    report.append(f"📥 Queue: {len(message_queue)}/{MAX_QUEUE_SIZE}")
    if not SILENT_MODE:
        await send_split_message_event(event, "\n".join(report))

@client.on(events.NewMessage(pattern=r'/setcustomheader (\S+) (.+)'))
async def set_custom_header(event):
    """Set custom header for a pair."""
    pair_name, header = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    header = header.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['custom_header'] = header
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"📑 Set custom header for '{pair_name}': {header}")

@client.on(events.NewMessage(pattern=r'/setcustomfooter (\S+) (.+)'))
async def set_custom_footer(event):
    """Set custom footer for a pair."""
    pair_name, footer = event.pattern_match.groups()
    user_id = str(event.sender_id)
    pair_name = pair_name.strip()
    footer = footer.strip()
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['custom_footer'] = footer
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"📑 Set custom footer for '{pair_name}': {footer}")

@client.on(events.NewMessage(pattern=r'/clearcustomheaderfooter (\S+)'))
async def clear_custom_header_footer(event):
    """Clear custom header and footer for a pair."""
    pair_name = event.pattern_match.group(1).strip()
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or pair_name not in channel_mappings[user_id]:
        await event.reply("❌ Pair not found.")
        return
    channel_mappings[user_id][pair_name]['custom_header'] = ''
    channel_mappings[user_id][pair_name]['custom_footer'] = ''
    save_mappings()
    if not SILENT_MODE:
        await event.reply(f"🗑️ Cleared custom header and footer for '{pair_name}'.")

@client.on(events.NewMessage(pattern='(?i)^/listpairs$'))
async def list_pairs(event):
    """List all forwarding pairs for the user."""
    user_id = str(event.sender_id)
    if user_id not in channel_mappings or not channel_mappings[user_id]:
        await event.reply("❌ No pairs found.")
        return
    pairs = [f"📌 {name}: {data['source']} ➡️ {data['destination']} [{data['status'].capitalize()}]"
             for name, data in channel_mappings[user_id].items()]
    if not SILENT_MODE:
        await event.reply("\n".join(pairs))

@client.on(events.NewMessage(pattern='(?i)^/clearpairs$'))
async def clear_pairs(event):
    """Remove all forwarding pairs for the user."""
    user_id = str(event.sender_id)
    if user_id not in channel_mappings:
        await event.reply("❌ No pairs found.")
        return
    channel_mappings[user_id] = {}
    pair_stats[user_id] = {}
    save_mappings()
    if not SILENT_MODE:
        await event.reply("🗑️ All pairs cleared.")

async def send_split_message_event(event, full_message):
    """Send long message as multiple parts."""
    if len(full_message) <= MAX_MESSAGE_LENGTH:
        await event.reply(full_message)
        return
    parts = [full_message[i:i + MAX_MESSAGE_LENGTH] for i in range(0, len(full_message), MAX_MESSAGE_LENGTH)]
    for i, part in enumerate(parts, 1):
        await event.reply(f"📜 Part {i}/{len(parts)}\n{part}")
        await asyncio.sleep(0.5)

@client.on(events.NewMessage)
async def copy_messages(event):
    """Queue new messages for copying."""
    if not is_connected:
        return
    queued_time = datetime.now()
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['status'] == 'active' and event.chat_id == int(mapping['source']):
                mapping['pair_name'] = pair_name
                message_queue.append((event, mapping, user_id, pair_name, queued_time))
                pair_stats[user_id][pair_name]['queued'] += 1
                if not mapping.get('stealth_mode', True):
                    logger.info(f"Message queued for pair '{pair_name}'")

@client.on(events.MessageEdited)
async def handle_message_edit(event):
    """Handle edited messages."""
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['status'] == 'active' and event.chat_id == int(mapping['source']):
                mapping['pair_name'] = pair_name
                await edit_copied_message(event, mapping, user_id, pair_name)
                return

@client.on(events.MessageDeleted)
async def handle_message_deleted(event):
    """Handle deleted messages."""
    if not is_connected:
        return
    for user_id, pairs in channel_mappings.items():
        for pair_name, mapping in pairs.items():
            if mapping['status'] == 'active' and event.chat_id == int(mapping['source']):
                for deleted_id in event.deleted_ids:
                    event.message.id = deleted_id
                    await delete_copied_message(event, mapping, user_id, pair_name)
                return

# Periodic Tasks
async def check_connection_status():
    """Monitor connection status."""
    global is_connected
    while True:
        current_status = client.is_connected()
        if current_status != is_connected:
            is_connected = current_status
            logger.info(f"📡 Connection {'established' if is_connected else 'lost'}")
        await asyncio.sleep(5)

async def queue_worker():
    """Process message queue."""
    while True:
        if is_connected and message_queue:
            try:
                event, mapping, user_id, pair_name, queued_time = message_queue.popleft()
                await copy_message_with_retry(event, mapping, user_id, pair_name)
            except Exception as e:
                logger.error(f"Queue worker error for pair '{pair_name}': {e}")
        await asyncio.sleep(0.1)

async def check_queue_inactivity():
    """Check for stuck messages in queue."""
    while True:
        await asyncio.sleep(60)
        if not is_connected or not NOTIFY_OWNER or not OWNER_ID or not message_queue:
            continue
        current_time = datetime.now()
        for event, _, _, pair_name, queued_time in message_queue:
            wait_duration = (current_time - queued_time).total_seconds()
            if wait_duration > QUEUE_INACTIVITY_THRESHOLD:
                source_msg_id = event.message.id if hasattr(event.message, 'id') else "Unknown"
                await client.send_message(
                    OWNER_ID,
                    f"⏳ Queue Inactivity Alert: Message for '{pair_name}' stuck for {int(wait_duration // 60)} minutes."
                )
                break

async def check_pair_inactivity():
    """Check for inactive pairs."""
    while True:
        await asyncio.sleep(300)
        if not is_connected or not NOTIFY_OWNER or not OWNER_ID:
            continue
        current_time = datetime.now()
        for user_id, pairs in channel_mappings.items():
            for pair_name, mapping in pairs.items():
                if mapping['status'] != 'active':
                    continue
                stats = pair_stats.get(user_id, {}).get(pair_name, {})
                last_activity = stats.get('last_activity')
                if last_activity:
                    last_activity_time = datetime.fromisoformat(last_activity)
                    if (current_time - last_activity_time).total_seconds() > INACTIVITY_THRESHOLD:
                        await client.send_message(
                            OWNER_ID,
                            f"⏰ Inactivity Alert: Pair '{pair_name}' inactive for over {INACTIVITY_THRESHOLD // 3600} hours."
                        )

async def send_periodic_report():
    """Send periodic reports."""
    while True:
        await asyncio.sleep(21600)
        if not is_connected or not NOTIFY_OWNER or not OWNER_ID:
            continue
        for user_id in channel_mappings:
            report = ["📊 6-Hour Report"]
            for pair_name, data in channel_mappings[user_id].items():
                stats = pair_stats.get(user_id, {}).get(pair_name, {})
                report.append(
                    f"📌 {pair_name}: {data['source']} ➡️ {data['destination']} [{data['status'].capitalize()}]\n"
                    f"   📈 Fwd: {stats.get('forwarded', 0)} | Edt: {stats.get('edited', 0)} | "
                    f"Del: {stats.get('deleted', 0)} | Blk: {stats.get('blocked', 0)}"
                )
            report.append(f"📥 Queue: {len(message_queue)}/{MAX_QUEUE_SIZE}")
            await client.send_message(OWNER_ID, "\n".join(report))

# Main Function
async def main():
    """Start the bot."""
    load_mappings()
    global worker_tasks
    tasks = [
        check_connection_status(),
        send_periodic_report(),
        check_pair_inactivity(),
        check_queue_inactivity()
    ]
    for _ in range(NUM_WORKERS):
        task = asyncio.create_task(queue_worker())
        worker_tasks.append(task)
    tasks.extend(worker_tasks)

    try:
        await client.start()
        if not await client.is_user_authorized():
            phone = input("Enter phone (or bot token): ")
            await client.start(phone=phone)
            code = input("Enter verification code: ")
            await client.sign_in(phone=phone, code=code)

        global is_connected, OWNER_ID
        is_connected = client.is_connected()
        OWNER_ID = (await client.get_me()).id
        logger.info(f"📡 Initial connection {'established' if is_connected else 'not established'}")

        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        save_mappings()

if __name__ == "__main__":
    try:
        client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("🤖 Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
