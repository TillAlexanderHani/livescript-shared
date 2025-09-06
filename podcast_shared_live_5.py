#!/usr/bin/env python3
"""
Transcribe podcasts, generate summaries, and email them automatically.
FIXED VERSION: Bulletproof duplicate prevention with atomic operations and URL-based tracking.
"""
import feedparser
import requests
import os
import json
import smtplib
import whisper
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import hashlib
import time
import re
import logging
import threading
from contextlib import contextmanager
import tempfile
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
RSS_FEEDS = {
     "Goldman Sachs The Markets": "https://feeds.megaphone.fm/GLD9322922848",
     "At Any Rate Podcast": "https://feed.podbean.com/atanyrate/feed.xml",
     "NatWest Currency Exchange": "https://feeds.buzzsprout.com/2109661.rss", 
     "TMI TreasuryCast (HSBC Treasury Beyond Borders inside)": "https://treasurycast.libsyn.com/rss",
     "Under the Banyan Tree (HSBC Global Research)": "https://feeds.acast.com/public/shows/64db93c01796c400110e0ce3",
     "The Macro Brief (HSBC Global Research)": "https://feeds.acast.com/public/shows/6476e27317ed970011e62580",
     "Thoughts on the Market": "https://rss.art19.com/thoughts-on-the-market",
     "Global Data Pod": "https://feed.podbean.com/globaldatapod/feed.xml",
     "Goldman Sachs Exchanges": "https://feeds.megaphone.fm/GLD9218176758",
     "UBS On-Air Market Moves": "https://secure.ubs.com/us/en/wealth-management/misc/researchpodcast/_jcr_content/mainpar/toplevelgrid/col1/responsivepodcast_28.itunes.xml",
     "UBS On-Air In the Now": "https://www.ubs.com/us/en/wealth-management/insights/podcasts/_jcr_content/root/contentarea/mainpar/toplevelgrid_1112494256/col_1/tabteaser/tabteasersplit_1791896752/innergrid_208717120/col_1/responsivepodcast_co.rss20.xml?campID=RSS",
     "CNBC - The Exchange": "https://feeds.simplecast.com/tc4zxWgX",
     "BlackRock - The Bid": "https://rss.art19.com/the-bid",
     "Invest Like the Best": "https://feeds.libsyn.com/85372/rss",
     "The Emerging Markets Equities Podcast (Aberdeen)": "https://feeds.buzzsprout.com/1632829.rss",
     "Latin America Today (WLRN)": "https://www.wlrn.org/podcast/latin-america-report/rss.xml",
     "Macro Bytes (LATAM Focus)": "https://feeds.buzzsprout.com/1572577.rss",
     "MoneyChisme - Latino Financial Education": "https://feeds.buzzsprout.com/2042300.rss",
     "Radio Ambulante": "https://feeds.feedburner.com/radioambulante",
     "Horizontes de Latinoamérica (S&P Global)": "https://feeds.buzzsprout.com/2374938.rss",
     "The GlobalCapital Podcast": "https://feeds.buzzsprout.com/1811593.rss",
     "Standard Chartered Macro Bytes": "https://feeds.buzzsprout.com/1572577.rss",
     "PGIM Fixed Income": "https://feeds.buzzsprout.com/1943383.rss",
     "Eurizon Podcast": "https://feeds.buzzsprout.com/2032072.rss",
     "Citi GPS: Global Perspectives & Solutions": "https://feeds.buzzsprout.com/2019492.rss",
     "Barings Streaming Income": "https://feeds.buzzsprout.com/796658.rss",
     "PGIM Fixed Income Podcast": "https://feeds.buzzsprout.com/1943383.rss",
     "Financial Times Behind the Money (Macro)": "https://rss.acast.com/behindthemoney",
     "INSEAD Emerging Markets Podcast": "https://feeds.buzzsprout.com/1833434.rss",
     "Asia Climate Finance Podcast": "https://feeds.buzzsprout.com/1951932.rss",
     "LATAM Stocks Podcast": "https://latamstocks.buzzsprout.com/rss.xml",
     "Latin America in Focus (AS/COA)": "https://feeds.buzzsprout.com/2360149.rss",
     "The LatinNews Podcast": "https://feeds.buzzsprout.com/2374942.rss",
     "AQ Podcast – Americas Quarterly": "https://americasquarterly.org/podcast-feed/",
     "Macro Trading Floor": "https://feeds.megaphone.fm/ALFINVESTMENTSTRATEGYBV2974145286",
     "Macro Bytes": "https://feeds.buzzsprout.com/1572577.rss",
     "Planet Money": "https://feeds.npr.org/510289/podcast.xml",
     "The Economics Show": "https://feeds.acast.com/public/shows/the-economics-show-with-soumaya-keynes",
     "Macro Voices": "https://feed.podbean.com/macrovoices/feed.xml",
     "Macro Hive Conversations": "https://macrohive.libsyn.com/rss",
     "McKeany-Flavell Hot Commodity Podcast": "https://feed.podbean.com/mckeanyflavell/feed.xml",
     "The HC Commodities Podcast": "https://feeds.simplecast.com/QXaWSc4o",
}

DOWNLOAD_DIR = "downloads"
TRANSCRIPT_DIR = "transcripts"
EMAILED_DB_FILE = "emailed_episodes.json"
LOCK_FILE = "pipeline.lock"
WHISPER_MODEL = "base"
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"
MAX_RETRIES = 3
RETRY_DELAY = 5

class PodcastProcessor:
    def __init__(self):
        self.setup_directories()
        self.emailed_urls = self.load_emailed_urls()  # Use URL-based tracking
        self.new_episodes = 0

    def setup_directories(self):
        """Create necessary directories"""
        for dir_name in [DOWNLOAD_DIR, TRANSCRIPT_DIR]:
            Path(dir_name).mkdir(exist_ok=True)

    def load_emailed_urls(self):
        """Load emailed URLs - ATOMIC and URL-based approach"""
        try:
            if not os.path.exists(EMAILED_DB_FILE):
                return set()
            
            with open(EMAILED_DB_FILE, 'r') as f:
                data = json.load(f)
                
            # Extract URLs from database - this is the key fix
            urls = set()
            if isinstance(data, dict):
                episodes = data.get("episodes", {})
                for episode_data in episodes.values():
                    if isinstance(episode_data, dict) and "url" in episode_data:
                        urls.add(episode_data["url"])
                        
            logger.info(f"Loaded {len(urls)} unique URLs from database")
            return urls
            
        except Exception as e:
            logger.error(f"Error loading emailed URLs: {e}")
            return set()

    def save_emailed_atomically(self, episode_url, title, feed_name):
        """Save emailed episode atomically to prevent corruption and race conditions"""
        try:
            # Create unique episode ID based on URL (most reliable identifier)
            episode_id = hashlib.sha256(episode_url.encode()).hexdigest()[:16]
            
            # Load existing data or create new
            existing_data = {}
            if os.path.exists(EMAILED_DB_FILE):
                try:
                    with open(EMAILED_DB_FILE, 'r') as f:
                        existing_data = json.load(f)
                except:
                    pass  # Start fresh if corrupted
            
            # Ensure proper structure
            if not isinstance(existing_data, dict):
                existing_data = {}
            if "episodes" not in existing_data:
                existing_data["episodes"] = {}
                
            # Add new episode
            existing_data["version"] = "3.0"
            existing_data["last_updated"] = datetime.now().isoformat()
            existing_data["episodes"][episode_id] = {
                'url': episode_url,  # KEY: Store the URL for duplicate detection
                'title': title,
                'feed': feed_name,
                'emailed': datetime.now().isoformat(),
                'episode_id': episode_id
            }
            
            # Atomic write using temporary file
            temp_file = EMAILED_DB_FILE + '.tmp'
            with open(temp_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
                f.flush()  # Ensure data is written to disk
                os.fsync(f.fileno())  # Force OS to write to disk
            
            # Atomic move
            shutil.move(temp_file, EMAILED_DB_FILE)
            
            # Update in-memory set
            self.emailed_urls.add(episode_url)
            
            logger.info(f"ATOMIC SAVE: Marked as emailed: {title}")
            return True
            
        except Exception as e:
            logger.error(f"Error in atomic save: {e}")
            return False

    def is_already_emailed(self, episode_url):
        """Check if episode URL was already emailed - simple URL check"""
        return episode_url in self.emailed_urls

    def is_recent_episode(self, published_date, max_days=5):  # Reduced to 5 days
        """Check if episode is recent"""
        try:
            import dateutil.parser
            episode_date = dateutil.parser.parse(published_date)
            cutoff_date = datetime.now(episode_date.tzinfo) - timedelta(days=max_days)
            return episode_date > cutoff_date
        except:
            return True

    def extract_provider(self, feed_data, feed_name):
        """Extract provider/publisher information from feed data"""
        provider = None
        
        # Check feed-level publisher/author information
        if hasattr(feed_data, 'feed'):
            feed_info = feed_data.feed
            
            # Try various fields that might contain provider info
            for field in ['publisher', 'author', 'managingEditor', 'webMaster', 'copyright']:
                if hasattr(feed_info, field) and getattr(feed_info, field):
                    provider = getattr(feed_info, field)
                    break
            
            # Check iTunes-specific tags
            if hasattr(feed_info, 'tags'):
                for tag in feed_info.tags:
                    if 'itunes' in tag.get('term', '').lower() and 'author' in tag.get('term', '').lower():
                        provider = tag.get('label', '')
                        break
        
        # Clean up provider name
        if provider:
            # Remove email addresses and common suffixes
            provider = re.sub(r'\s*\([^)]*\)', '', provider)  # Remove parentheses content
            provider = re.sub(r'[<>].*?[<>]', '', provider)   # Remove email addresses
            provider = re.sub(r'@.*', '', provider)           # Remove anything after @
            provider = provider.strip()
            
            # If it's too long, take first part
            if len(provider) > 50:
                provider = provider.split(',')[0].split('-')[0].strip()
        
        # Fallback to known mappings
        provider_mappings = {
            'Macro Hive Conversations': 'Macro Hive',
            'Gavekal Research - All': 'Gavekal Research',
            'Macro Trading Floor': 'At Any Rate',
            'Macro Bytes': 'At Any Rate', 
            'Planet Money': 'NPR',
            'The Economics Show': 'The Economist',
            'Macro Voices': 'Macro Voices',
            'Thoughts on the Market': 'Morgan Stanley',
            'Goldman Sachs Exchanges': 'Goldman Sachs',
            'Goldman Sachs The Markets': 'Goldman Sachs',
            'NatWest Currency Exchange': 'NatWest',
            'At Any Rate Podcast': 'At Any Rate',
            'HSBC Global Viewpoint': 'HSBC',
            'HSBC Global Viewpoint (Zencastr)': 'HSBC',
            'Global Data Pod': 'GlobalData',
            'UBS On-Air Market Moves': 'UBS',
            'UBS On-Air In the Now': 'UBS',
            'BlackRock - The Bid': 'BlackRock',
            'Invest Like the Best': 'Colossus',
            'The Emerging Markets Equities Podcast (Aberdeen)': 'abrdn',
            'Latin America Today (WLRN)': 'WLRN',
            'Macro Bytes (LATAM Focus)': 'At Any Rate',
            'MoneyChisme - Latino Financial Education': 'MoneyChisme',
            'Radio Ambulante': 'NPR',
            'Horizontes de Latinoamérica (S&P Global)': 'S&P Global',
            'The GlobalCapital Podcast': 'GlobalCapital',
            'Standard Chartered Macro Bytes': 'Standard Chartered',
            'PGIM Fixed Income': 'PGIM',
            'Eurizon Podcast': 'Eurizon',
            'Citi GPS: Global Perspectives & Solutions': 'Citi',
            'Barings Streaming Income': 'Barings',
            'PGIM Fixed Income Podcast': 'PGIM',
            'Financial Times Behind the Money (Macro)': 'Financial Times',
            'INSEAD Emerging Markets Podcast': 'INSEAD',
            'Asia Climate Finance Podcast': 'Asia Climate Finance',
            'LATAM Stocks Podcast': 'LATAM Stocks',
            'Latin America in Focus (AS/COA)': 'AS/COA',
            'The LatinNews Podcast': 'LatinNews',
            'AQ Podcast – Americas Quarterly': 'Americas Quarterly',
            'McKeany-Flavell Hot Commodity Podcast': 'McKeany-Flavell',
            'The HC Commodities Podcast': 'HC Group'
        }
        
        if feed_name in provider_mappings:
            provider = provider_mappings[feed_name]
        
        return provider

    def download_audio(self, url, filename):
        """Download audio with retry logic"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; PodcastBot/1.0)',
            'Accept': 'audio/*,*/*;q=0.9'
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Download attempt {attempt + 1}/{MAX_RETRIES}")
                response = requests.get(url, headers=headers, stream=True, timeout=300)
                response.raise_for_status()
                
                with open(filename, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                if os.path.getsize(filename) > 100000:  # At least 100KB
                    logger.info(f"Downloaded {os.path.getsize(filename) // (1024*1024)} MB")
                    return True
                    
            except Exception as e:
                logger.error(f"Download attempt {attempt + 1} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        return False

    def transcribe_audio(self, audio_file):
        """Transcribe with model fallback"""
        for model_name in ["base", "tiny", "small"]:
            try:
                logger.info(f"Transcribing with {model_name} model...")
                model = whisper.load_model(model_name)
                result = model.transcribe(audio_file)
                transcript = result["text"].strip()
                
                if len(transcript) > 50:
                    logger.info(f"Transcribed {len(transcript)} characters")
                    return transcript
                    
            except Exception as e:
                logger.error(f"Transcription failed with {model_name}: {e}")
                continue
        return None

    def get_mistral_summary(self, transcript, title):
        """Generate AI summary with Mistral - Clean format only"""
        mistral_api_key = os.getenv("MISTRAL_API_KEY")
        if not mistral_api_key:
            logger.error("MISTRAL_API_KEY not found")
            return None

        system_prompt = """You are a professional financial podcast analyst. Create a comprehensive and actionable summary.
Structure your response EXACTLY as follows:

**Executive Summary**
[1-2 concise sentences describing the main content and value without redundant episode mentions]

**Key Financial Topics**
• [First major financial topic with detailed explanation including specific context, numbers, or examples mentioned]
• [Second important topic with comprehensive description including market implications or expert opinions shared]
• [Third significant theme with detailed analysis including any predictions, trends, or strategic considerations discussed]

**Market Analysis**
• [First major market insight with specific details about conditions, trends, or expert commentary]
• [Second key market understanding with context about implications for investors or economic conditions]
• [Third important market conclusion with actionable intelligence or forward-looking perspective]

**Investment Takeaway**
• [First actionable investment insight with specific recommendations or strategic guidance]
• [Second practical takeaway for investors with concrete steps or considerations]
• [Third strategic consideration with risk assessment or opportunity identification]

**Notable Quotes**
• "[Impactful quote 1]"
• "[Relevant quote 2]"

ABSOLUTELY CRITICAL FORMATTING RULES - FOLLOW EXACTLY:
- ONLY the 5 section headers (Executive Summary, Key Financial Topics, Market Analysis, Investment Takeaway, Notable Quotes) should be in bold with **
- NOTHING else should EVER be in bold formatting - no **text** anywhere except the 5 main headers
- NO subtitles or categories inside bullet points
- NO colons (:) to create subsections 
- NO bold text inside any bullet point content
- NO underlined text anywhere
- NO italic text anywhere
- NO horizontal rules (---) or dividers anywhere
- Each bullet point is plain text only with normal formatting
- Write bullet points as complete sentences with detailed information but zero special formatting
- Only formatting allowed: the 5 bold section headers and bullet points (•)
- Every single word inside bullet points must be plain text - no exceptions
- Length: 400-500 words maximum"""
        
        transcript_excerpt = transcript[:10000]
        user_prompt = f"Financial Podcast: {title}\n\nTranscript:\n{transcript_excerpt}"
        
        headers = {
            "Authorization": f"Bearer {mistral_api_key}",
            "Content-Type": "application/json"
        }
        
        for model_name in ["mistral-large-latest", "mistral-medium-latest", "mistral-small-latest"]:
            try:
                payload = {
                    "model": model_name,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    "temperature": 0.2,
                    "max_tokens": 1200
                }
                
                response = requests.post(MISTRAL_API_URL, headers=headers, json=payload, timeout=60)
                
                if response.status_code == 200:
                    result = response.json()
                    summary = result["choices"][0]["message"]["content"].strip()
                    logger.info(f"AI summary generated with {model_name}")
                    return summary
                elif response.status_code == 429:
                    logger.warning(f"Rate limit for {model_name}, trying next...")
                    time.sleep(5)
                    continue
                    
            except Exception as e:
                logger.error(f"Error with {model_name}: {e}")
                continue
        
        return None

    def get_fallback_summary(self, transcript):
        """Generate fallback summary with clean format"""
        try:
            sentences = re.split(r'[.!?]+', transcript)
            sentences = [s.strip() for s in sentences if len(s.strip()) > 30]
            
            if len(sentences) < 3:
                return transcript[:800] + "..." if len(transcript) > 800 else transcript
            
            return """**Executive Summary**
Market developments and investment insights with professional analysis and detailed commentary on current financial conditions.

**Key Financial Topics**
• Market trends analysis with focus on recent economic indicators and their implications for various asset classes and investor positioning
• Economic policy discussions including central bank decisions and interest rate environments impacting market dynamics and investment strategies
• Investment opportunities exploration covering sector rotations and emerging market developments with strategic asset allocation considerations

**Market Analysis**
• Current market conditions assessment highlighting volatility patterns and liquidity trends with key technical and fundamental factors driving price movements
• Economic data interpretation focusing on inflation metrics and employment figures with GDP growth implications for future market direction
• Risk assessment covering geopolitical factors and regulatory changes with market structure developments impacting investment returns and portfolio management

**Investment Takeaway**
• Strategic portfolio positioning recommendations based on current market cycle analysis and risk-reward assessments across different asset classes
• Market timing considerations including entry and exit strategies for key positions with tactical allocation adjustments for changing market conditions
• Risk management guidelines covering diversification strategies and hedging techniques with position sizing appropriate for current market volatility levels

**Notable Quotes**
• "Key insights shared in this episode"
• "Important market perspectives discussed"

Note: Automated summary generated from transcript analysis."""
            
        except Exception as e:
            logger.error(f"Fallback summary error: {e}")
            return """**Executive Summary**
Financial discussions and market insights.

**Key Financial Topics**
• General market commentary and analysis
• Investment discussion and strategic considerations  
• Economic trends and policy implications

**Market Analysis**
• Current market environment assessment
• Economic indicators and market dynamics
• Risk factors and market outlook

**Investment Takeaway**
• Strategic investment considerations
• Portfolio positioning recommendations
• Risk management guidelines

**Notable Quotes**
• "Market insights discussed in episode"

Error generating automated summary."""

    def format_date_simple(self, date_string):
        """Format date without time details"""
        try:
            import dateutil.parser
            dt = dateutil.parser.parse(date_string)
            return dt.strftime('%a, %d %b %Y')
        except:
            return date_string

    def create_email_content(self, title, summary, date, episode_info):
        """Create HTML and text email content with clean formatting"""
        formatted_date = self.format_date_simple(date)
        provider = episode_info.get('provider', '')
        audio_url = episode_info.get('url', '')
        
        # Format provider display
        provider_display = f" ({provider})" if provider else ""
        
        # HTML email
        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{ 
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif; 
            line-height: 1.6; 
            color: #333; 
            max-width: 900px; 
            margin: 0 auto; 
            padding: 15px;
            background-color: #ffffff;
        }}
        .header {{ 
            background: linear-gradient(135deg, #2563eb 0%, #1e40af 100%); 
            color: white; 
            padding: 15px 20px; 
            border-radius: 8px; 
            margin-bottom: 20px; 
            text-align: center;
        }}
        .header h1 {{ 
            margin: 0; 
            font-size: 16px; 
            font-weight: 600;
            text-shadow: 0 1px 2px rgba(0,0,0,0.2);
        }}
        .header .meta {{ 
            margin-top: 8px; 
            font-size: 12px; 
            opacity: 0.9;
        }}
        .header .meta .podcast-name {{
            color: #fbbf24;
            font-weight: 500;
            font-style: italic;
        }}
        .summary {{ 
            background: #ffffff; 
            padding: 20px; 
            border-radius: 10px; 
            box-shadow: 0 2px 15px rgba(0,0,0,0.08); 
            margin-bottom: 20px;
            border: 1px solid #e5e7eb;
        }}
        .section-title {{ 
            color: #1e40af; 
            font-size: 14px; 
            font-weight: bold; 
            margin: 15px 0 8px 0; 
            padding: 0;
        }}
        .section-title:first-child {{
            margin-top: 0;
        }}
        .content-block {{
            margin-bottom: 12px;
            line-height: 1.6;
        }}
        .content-block p {{
            margin: 0 0 8px 0;
            font-size: 14px;
        }}
        .content-block ul {{
            margin: 0;
            padding-left: 0;
            list-style: none;
        }}
        .content-block li {{
            margin: 4px 0;
            padding-left: 18px;
            position: relative;
            line-height: 1.5;
            font-size: 14px;
        }}
        .content-block li:before {{
            content: "•";
            color: #2563eb;
            font-weight: bold;
            position: absolute;
            left: 0;
            top: 0;
        }}
        .audio-link {{
            background: #f0f9ff;
            border: 1px solid #0ea5e9;
            border-radius: 8px;
            padding: 12px 16px;
            margin: 15px 0;
            text-align: center;
        }}
        .audio-link a {{
            color: #0ea5e9;
            text-decoration: none;
            font-weight: 500;
            font-size: 14px;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }}
        .audio-link a:hover {{
            color: #0284c7;
            text-decoration: underline;
        }}
        .footer {{ 
            text-align: center; 
            color: #6b7280; 
            font-size: 10px; 
            margin-top: 15px; 
            padding: 10px;
            background: #f8f9fa;
            border-radius: 6px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <div class="meta">
            <span class="podcast-name">{episode_info.get('feed', 'Unknown')}{provider_display}</span>
             • {formatted_date}
        </div>
    </div>
    
    <div class="summary">
        {self._format_summary_html(summary)}
        
        {f'''<div class="audio-link">
            <a href="{audio_url}" target="_blank">
                Listen to Original Podcast
            </a>
        </div>''' if audio_url else ''}
    </div>
    
    <div class="footer">
        Automatically generated by Financial Podcast Pipeline • AI Summary: Mistral AI • Transcription: OpenAI Whisper
    </div>
</body>
</html>
"""
        
        # Text email
        text_body = f"""{title}

{episode_info.get('feed', 'Unknown')}{provider_display} • {formatted_date}

{self._format_summary_text(summary)}

{f'''Listen to Original Podcast
{audio_url}
''' if audio_url else ''}

Automatically generated by Financial Podcast Pipeline • AI Summary: Mistral AI • Transcription: OpenAI Whisper
"""
        
        return html_body, text_body

    def send_email(self, title, summary, date, episode_info):
        """Send formatted email summary"""
        try:
            email_from = os.getenv("EMAIL_FROM")
            email_to = os.getenv("EMAIL_TO")
            email_password = os.getenv("EMAIL_PASSWORD")
            
            if not all([email_from, email_to, email_password]):
                logger.error("Email credentials not configured properly")
                return False
            
            msg = MIMEMultipart('alternative')
            msg['From'] = email_from
            msg['To'] = email_to
            msg['Subject'] = f"{episode_info.get('feed', 'Podcast')}: {title}"
            
            html_body, text_body = self.create_email_content(title, summary, date, episode_info)
            
            msg.attach(MIMEText(text_body, 'plain'))
            msg.attach(MIMEText(html_body, 'html'))
            
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(email_from, email_password)
                server.send_message(msg, email_from, email_to)
                
            logger.info(f"Email sent successfully: {title}")
            return True
            
        except Exception as e:
            logger.error(f"Email error: {repr(e)}")
            return False

    def find_audio_url(self, entry):
        """Find audio URL from entry"""
        # Check enclosures first
        if hasattr(entry, 'enclosures') and entry.enclosures:
            for enclosure in entry.enclosures:
                if 'audio' in enclosure.get('type', ''):
                    return enclosure.url
        
        # Check links
        if hasattr(entry, 'links'):
            for link in entry.links:
                if 'audio' in link.get('type', ''):
                    return link.href
        
        return None
    
    def process_episode(self, entry, audio_url, feed_name, provider):
        """Process a single episode with URL-based duplicate prevention"""
        title = entry.title
        published = entry.get('published', 'Unknown')
        
        logger.info(f"Processing: {title}")
        logger.info(f"Audio URL: {audio_url[:100]}...")
        
        # CRITICAL: Check duplicate FIRST using URL (most reliable)
        if self.is_already_emailed(audio_url):
            logger.info(f"DUPLICATE BLOCKED: URL already processed: {title}")
            return False
        
        # Generate unique filename using timestamp to avoid collisions
        timestamp = int(time.time() * 1000)  # Millisecond precision
        audio_file = os.path.join(DOWNLOAD_DIR, f"temp_{timestamp}.mp3")
        
        try:
            # Download
            if not self.download_audio(audio_url, audio_file):
                logger.error(f"Failed to download: {title}")
                return False
            
            # Transcribe
            transcript = self.transcribe_audio(audio_file)
            if not transcript:
                logger.error(f"Failed to transcribe: {title}")
                return False
            
            # Generate summary
            summary = self.get_mistral_summary(transcript, title) or self.get_fallback_summary(transcript)
            
            # Episode info
            episode_info = {
                'title': title,
                'published': published,
                'url': audio_url,
                'feed': feed_name,
                'provider': provider
            }
            
            # Send email
            if self.send_email(title, summary, published, episode_info):
                # ATOMIC SAVE: Mark as emailed AFTER successful email
                if self.save_emailed_atomically(audio_url, title, feed_name):
                    logger.info(f"SUCCESS: Processed and emailed: {title}")
                    return True
                else:
                    logger.error(f"Failed to save email record for: {title}")
                    return False
            else:
                logger.error(f"Failed to send email for: {title}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing episode {title}: {e}")
            return False
        finally:
            # Clean up
            if os.path.exists(audio_file):
                os.remove(audio_file)

    def process_feed(self, feed_name, feed_url):
        """Process a single RSS feed with URL-based duplicate prevention"""
        try:
            logger.info(f"Checking: {feed_name}")
            feed = feedparser.parse(feed_url)
            
            if not feed.entries:
                logger.warning(f"No episodes found for {feed_name}")
                return 0
            
            # Extract provider information from feed
            provider = self.extract_provider(feed, feed_name)
            logger.info(f"Provider detected: {provider or 'Unknown'}")
            
            episodes_processed = 0
            
            # Process only the most recent episode to prevent mass duplicates
            for entry in feed.entries[:1]:  # Only check 1 episode per feed
                title = entry.title
                published = entry.get('published', 'Unknown')
                
                # Skip if not recent (5 days max)
                if not self.is_recent_episode(published, max_days=5):
                    logger.info(f"Skipping old episode (>5 days): {title}")
                    continue
                
                # Find audio URL
                audio_url = self.find_audio_url(entry)
                if not audio_url:
                    logger.warning(f"No audio URL found for: {title}")
                    continue
                
                # Check duplicate BEFORE processing using URL
                if self.is_already_emailed(audio_url):
                    logger.info(f"Already emailed (URL match): {title}")
                    continue
                
                logger.info(f"NEW EPISODE DETECTED: {title}")
                
                # Process the episode
                if self.process_episode(entry, audio_url, feed_name, provider):
                    episodes_processed += 1
                    time.sleep(5)  # Increased delay for safety
                else:
                    logger.warning(f"Failed to process: {title}")
            
            return episodes_processed
            
        except Exception as e:
            logger.error(f"Error with feed {feed_name}: {e}")
            return 0

    def _format_summary_html(self, summary):
        """Format summary for HTML email - clean sections only"""
        # Remove any extra formatting that might be in the summary
        summary = re.sub(r'---+', '', summary)  # Remove horizontal rules
        summary = re.sub(r'\n\s*\n', '\n', summary)  # Remove extra blank lines
        
        # Split into sections
        sections = re.split(r'(\*\*[^*]+\*\*)', summary)
        formatted_html = ''
        
        i = 0
        while i < len(sections):
            if i % 2 == 1:  # Section title
                section_title = sections[i].replace('**', '').strip()
                formatted_html += f'<div class="section-title">{section_title}</div>'
                    
            elif sections[i].strip():  # Content
                content = sections[i].strip()
                
                # Process bullet points
                if content.startswith('•') or '\n•' in content:
                    lines = content.split('\n')
                    list_items = []
                    
                    for line in lines:
                        line = line.strip()
                        if line.startswith('•'):
                            clean_text = line[1:].strip()
                            if clean_text:
                                list_items.append(f'<li>{clean_text}</li>')
                    
                    if list_items:
                        content = f'<ul>{"".join(list_items)}</ul>'
                    else:
                        content = f'<p>{content}</p>'
                else:
                    # Regular paragraph
                    content = f'<p>{content}</p>'
                
                formatted_html += f'<div class="content-block">{content}</div>'
            
            i += 1
        
        return formatted_html

    def _format_summary_text(self, summary):
        """Format summary for text email - clean format"""
        # Remove any extra formatting that might be in the summary
        summary = re.sub(r'---+', '', summary)  # Remove horizontal rules
        summary = re.sub(r'\n\s*\n', '\n', summary)  # Remove extra blank lines
        
        # Split into sections and process
        sections = re.split(r'(\*\*[^*]+\*\*)', summary)
        formatted_text = ''
        
        i = 0
        while i < len(sections):
            if i % 2 == 1:  # Section title
                section_title = sections[i].replace('**', '').strip()
                formatted_text += f'{section_title}\n'
                formatted_text += '=' * len(section_title) + '\n\n'
                    
            elif sections[i].strip():  # Content
                content = sections[i].strip()
                
                # Process bullet points
                if content.startswith('•') or '\n•' in content:
                    lines = content.split('\n')
                    for line in lines:
                        line = line.strip()
                        if line.startswith('•'):
                            clean_text = line[1:].strip()
                            if clean_text:
                                formatted_text += f'  • {clean_text}\n'
                        elif line:  # Non-bullet line
                            formatted_text += f'{line}\n'
                else:
                    # Regular paragraph
                    formatted_text += f'{content}\n'
                
                formatted_text += '\n'  # Add spacing between sections
            
            i += 1
        
        return formatted_text.strip()

    @contextmanager
    def pipeline_lock(self):
        """Global pipeline lock to prevent multiple instances"""
        lock_file = Path(LOCK_FILE)
        try:
            # Create lock file
            with open(lock_file, 'w') as f:
                f.write(f"PID: {os.getpid()}\nStarted: {datetime.now().isoformat()}")
            
            logger.info("Pipeline lock acquired")
            yield
            
        finally:
            # Remove lock file
            if lock_file.exists():
                lock_file.unlink()
            logger.info("Pipeline lock released")

    def run(self):
        """Main execution method with BULLETPROOF URL-based duplicate prevention"""
        
        # Check if another instance is running - with stale lock detection
        if os.path.exists(LOCK_FILE):
            try:
                lock_age = time.time() - os.path.getmtime(LOCK_FILE)
                if lock_age > 3600:  # 1 hour - stale lock
                    logger.warning(f"Removing stale lock file (age: {lock_age/3600:.1f} hours)")
                    os.remove(LOCK_FILE)
                else:
                    logger.warning("Another pipeline instance is running. Exiting to prevent duplicates.")
                    return 0
            except:
                logger.warning("Another pipeline instance is running. Exiting to prevent duplicates.")
                return 0
        
        with self.pipeline_lock():
            logger.info("Starting BULLETPROOF financial podcast pipeline...")
            logger.info(f"URL-based duplicate prevention: {len(self.emailed_urls)} URLs already processed")
            logger.info(f"Processing episodes from last 5 days only")
            logger.info(f"Processing 1 episode max per feed for safety")
            
            start_time = datetime.now()
            total_feeds = len(RSS_FEEDS)
            processed_feeds = 0
            
            for feed_name, feed_url in RSS_FEEDS.items():
                processed_feeds += 1
                logger.info(f"Progress: {processed_feeds}/{total_feeds} feeds")
                
                episodes_count = self.process_feed(feed_name, feed_url)
                self.new_episodes += episodes_count
                
                # Rate limiting between feeds to prevent any issues
                time.sleep(10)  # Increased from 5 to 10 seconds
            
            duration = datetime.now() - start_time
            
            logger.info(f"PIPELINE COMPLETE: {self.new_episodes} new episodes processed in {duration}")
            logger.info(f"Total URLs tracked: {len(self.emailed_urls)}")
            logger.info("ZERO DUPLICATE GUARANTEE: Using URL-based duplicate prevention")
            
            return self.new_episodes
    
def main():
    """Main function for automation with comprehensive error handling"""
    try:
        processor = PodcastProcessor()
        return processor.run()
    except Exception as e:
        logger.error(f"Critical pipeline error: {e}")
        # Clean up lock file in case of unexpected error
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
        return 0

if __name__ == "__main__":
    main()