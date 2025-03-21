import os
import base64
import re
import json
import abc
import logging
from typing import Dict, List, Optional, Any, Union, Tuple, Type, Protocol
from datetime import datetime
import pathlib
from dataclasses import dataclass, field

# Common libraries
from bs4 import BeautifulSoup

# Gmail specific
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Microsoft Graph (Outlook) specific
# from msgraph.core import GraphClient
import msal


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Abstract base classes for authentication and email fetching
class EmailAuthenticator(abc.ABC):
    """Abstract base class for email service authentication."""
    
    @abc.abstractmethod
    def authenticate(self) -> Any:
        """Authenticate with the email service and return the client."""
        pass


class EmailFetcher(abc.ABC):
    """Abstract base class for fetching emails."""
    
    @abc.abstractmethod
    def fetch_emails(self, query: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """Fetch emails matching the given query."""
        pass
    
    @abc.abstractmethod
    def get_header_value(self, message: Dict[str, Any], name: str) -> Optional[str]:
        """Get the value of a specific header from a message."""
        pass
    
    @abc.abstractmethod
    def get_email_content(self, message: Dict[str, Any]) -> Optional[str]:
        """Extract the content from an email message."""
        pass
    
    @abc.abstractmethod
    def get_attachments(self, message: Dict[str, Any], download_dir: str) -> List[Dict[str, str]]:
        """Download attachments from an email message."""
        pass


# Gmail implementations
class GmailAuthenticator(EmailAuthenticator):
    """Gmail-specific authenticator implementation."""
    
    def __init__(
        self, 
        client_secrets_file: str,
        token_file: str = 'token.json',
        scopes: List[str] = None
    ):
        """
        Initialize the Gmail authenticator.
        
        Args:
            client_secrets_file: Path to the client secrets file
            token_file: Path to the token file for storing credentials
            scopes: The OAuth scopes to request
        """
        self.client_secrets_file = client_secrets_file
        self.token_file = token_file
        self.scopes = scopes or ['https://www.googleapis.com/auth/gmail.readonly']
        self.service = None
    
    def authenticate(self) -> Any:
        """
        Authenticate with Gmail API and return the service.
        
        Returns:
            The Gmail API service object
            
        Raises:
            FileNotFoundError: If the client secrets file is not found
            Exception: If authentication fails
        """
        if self.service:
            return self.service
            
        creds = None
        
        # Load existing credentials if available
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as token:
                    creds = Credentials.from_authorized_user_info(json.loads(token.read()))
            except Exception as e:
                logger.error(f"Error loading credentials: {e}")
        
        # Refresh or create new credentials if needed
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(self.client_secrets_file):
                    raise FileNotFoundError(f"Client secrets file not found: {self.client_secrets_file}")
                    
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.client_secrets_file, self.scopes)
                creds = flow.run_local_server(port=0)
            
            # Save the credentials for the next run
            with open(self.token_file, 'w') as token:
                token.write(creds.to_json())
        
        self.service = build('gmail', 'v1', credentials=creds)
        return self.service


class OutlookAuthenticator(EmailAuthenticator):
    """Outlook-specific authenticator implementation using Microsoft Graph API."""
    
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        token_file: str = 'outlook_token.json',
        scopes: List[str] = None
    ):
        """
        Initialize the Outlook authenticator.
        
        Args:
            client_id: Azure AD app client ID
            client_secret: Azure AD app client secret
            tenant_id: Azure AD tenant ID
            token_file: Path to the token file for storing credentials
            scopes: The OAuth scopes to request
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.token_file = token_file
        self.scopes = scopes or ['https://graph.microsoft.com/Mail.Read']
        self.client = None
    
    def authenticate(self) -> Any:
        """
        Authenticate with Microsoft Graph API and return the client.
        
        Returns:
            The Microsoft Graph client
        """
        if self.client:
            return self.client
        
        # Initialize the MSAL app
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            client_credential=self.client_secret
        )
        
        # Try to load token from cache
        token = None
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    token_cache = json.load(f)
                    account = token_cache.get('account')
                    if account:
                        # Try to get token silently
                        token = app.acquire_token_silent(
                            self.scopes, 
                            account=account
                        )
            except Exception as e:
                logger.error(f"Error loading token cache: {e}")
        
        # If no token, get one interactively
        if not token:
            # For service account flows or daemon apps
            token = app.acquire_token_for_client(scopes=self.scopes)
            
            # Save token to cache
            if 'access_token' in token:
                with open(self.token_file, 'w') as f:
                    token_cache = {
                        'token': token,
                        'account': app.get_accounts()[0] if app.get_accounts() else None
                    }
                    json.dump(token_cache, f)
        
        if 'access_token' not in token:
            raise Exception(f"Failed to acquire token: {token.get('error_description', 'Unknown error')}")
        
        # Create Microsoft Graph client
        self.client = GraphClient(credential=lambda: token['access_token'])
        return self.client


class GmailFetcher(EmailFetcher):
    """Gmail-specific email fetcher implementation."""
    
    def __init__(self, authenticator: GmailAuthenticator):
        """
        Initialize the Gmail fetcher.
        
        Args:
            authenticator: GmailAuthenticator instance
        """
        self.authenticator = authenticator
    
    def fetch_emails(self, query: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch emails matching the given query from Gmail.
        
        Args:
            query: Gmail search query
            max_results: Maximum number of emails to fetch
            
        Returns:
            List of message dictionaries
        """
        try:
            service = self.authenticator.authenticate()
            
            results = service.users().messages().list(
                userId='me', 
                q=query,
                maxResults=max_results
            ).execute()
            
            messages = results.get('messages', [])
            
            # Fetch full message details for each message
            full_messages = []
            for message in messages:
                msg = service.users().messages().get(
                    userId='me', 
                    id=message['id']
                ).execute()
                full_messages.append(msg)
                
            return full_messages
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            raise Exception(f"Error fetching emails: {e}")
    
    def get_header_value(self, message: Dict[str, Any], name: str) -> Optional[str]:
        """
        Get the value of a specific header from a Gmail message.
        
        Args:
            message: The message dictionary
            name: The header name to look for
            
        Returns:
            The header value, or None if not found
        """
        headers = message.get('payload', {}).get('headers', [])
        for header in headers:
            if header.get('name', '').lower() == name.lower():
                return header.get('value')
        return None
    
    def get_email_content(self, message: Dict[str, Any]) -> Optional[str]:
        """
        Extract the HTML or text content from a Gmail message.
        
        Args:
            message: The message dictionary
            
        Returns:
            The email content, or None if not found
        """
        payload = message.get('payload', {})
        
        # Helper function to decode a part
        def decode_part(part):
            if 'data' in part.get('body', {}):
                try:
                    return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                except Exception as e:
                    logger.error(f"Error decoding part: {e}")
                    return None
            return None
        
        # Helper function to recursively find content by mime type
        def find_content_by_type(part, mime_type):
            if part.get('mimeType') == mime_type:
                content = decode_part(part)
                if content:
                    return content
            
            if 'parts' in part:
                for subpart in part['parts']:
                    content = find_content_by_type(subpart, mime_type)
                    if content:
                        return content
            return None
        
        # Try to find HTML content first
        content = find_content_by_type(payload, 'text/html')
        
        # If no HTML content, try to find plain text
        if not content:
            content = find_content_by_type(payload, 'text/plain')
        
        # If still no content, try to decode the payload directly
        if not content:
            content = decode_part(payload)
            
        return content
    
    def get_attachments(self, message: Dict[str, Any], download_dir: str = './attachments') -> List[Dict[str, str]]:
        """
        Download attachments from a Gmail message.
        
        Args:
            message: The message dictionary
            download_dir: Directory to save attachments
            
        Returns:
            List of dictionaries with attachment information
        """
        try:
            service = self.authenticator.authenticate()
            message_id = message['id']
            
            # Create download directory if it doesn't exist
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
            
            # Helper function to find and download attachments recursively
            def process_parts(parts, attachments=None):
                if attachments is None:
                    attachments = []
                
                for part in parts:
                    filename = part.get('filename')
                    mime_type = part.get('mimeType')
                    part_id = part.get('body', {}).get('attachmentId')
                    
                    # Check if this part has a filename (indicating it's an attachment)
                    if filename and part_id:
                        # Get the attachment
                        attachment = service.users().messages().attachments().get(
                            userId='me',
                            messageId=message_id,
                            id=part_id
                        ).execute()
                        
                        # Decode attachment data
                        data = base64.urlsafe_b64decode(attachment['data'])
                        
                        # Create a safe filename
                        safe_filename = os.path.basename(''.join(c for c in filename if c.isalnum() or c in '._- '))
                        
                        # Ensure unique filename
                        file_path = os.path.join(download_dir, safe_filename)
                        if os.path.exists(file_path):
                            name, ext = os.path.splitext(safe_filename)
                            safe_filename = f"{name}_{message_id}{ext}"
                            file_path = os.path.join(download_dir, safe_filename)
                        
                        # Save the attachment
                        with open(file_path, 'wb') as f:
                            f.write(data)
                        
                        attachments.append({
                            'filename': safe_filename,
                            'path': file_path,
                            'mime_type': mime_type,
                            'size': len(data)
                        })
                    
                    # Check for nested parts
                    if 'parts' in part:
                        process_parts(part['parts'], attachments)
                
                return attachments
            
            # Start processing from the payload
            payload = message.get('payload', {})
            if 'parts' in payload:
                return process_parts(payload['parts'])
            
            return []
        
        except Exception as e:
            logger.error(f"Error downloading attachments: {e}")
            return []


class OutlookFetcher(EmailFetcher):
    """Outlook-specific email fetcher implementation using Microsoft Graph API."""
    
    def __init__(self, authenticator: OutlookAuthenticator):
        """
        Initialize the Outlook fetcher.
        
        Args:
            authenticator: OutlookAuthenticator instance
        """
        self.authenticator = authenticator
    
    def fetch_emails(self, query: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch emails matching the given query from Outlook.
        
        Args:
            query: Outlook search query
            max_results: Maximum number of emails to fetch
            
        Returns:
            List of message dictionaries
        """
        try:
            client = self.authenticator.authenticate()
            
            # Construct the Microsoft Graph API request
            # Note: Graph API has different query syntax than Gmail
            request_url = f"/me/messages?$filter=contains(subject,'{query}')&$top={max_results}&$expand=attachments"
            
            # Make the request
            response = client.get(request_url)
            
            if response.status_code == 200:
                results = response.json()
                return results.get('value', [])
            else:
                logger.error(f"Error fetching emails: {response.status_code} - {response.text}")
                raise Exception(f"Error fetching emails: {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching emails: {e}")
            raise Exception(f"Error fetching emails: {e}")
    
    def get_header_value(self, message: Dict[str, Any], name: str) -> Optional[str]:
        """
        Get the value of a specific header from an Outlook message.
        
        Args:
            message: The message dictionary
            name: The header name to look for
            
        Returns:
            The header value, or None if not found
        """
        # Microsoft Graph provides common headers directly in the message object
        if name.lower() == 'subject':
            return message.get('subject')
        elif name.lower() == 'from':
            from_info = message.get('from', {}).get('emailAddress', {})
            return f"{from_info.get('name')} <{from_info.get('address')}>"
        elif name.lower() == 'to':
            to_recipients = message.get('toRecipients', [])
            if to_recipients:
                to_emails = [f"{r.get('emailAddress', {}).get('name')} <{r.get('emailAddress', {}).get('address')}>" 
                            for r in to_recipients]
                return '; '.join(to_emails)
        elif name.lower() == 'cc':
            cc_recipients = message.get('ccRecipients', [])
            if cc_recipients:
                cc_emails = [f"{r.get('emailAddress', {}).get('name')} <{r.get('emailAddress', {}).get('address')}>" 
                            for r in cc_recipients]
                return '; '.join(cc_emails)
        
        # For other headers, we need to fetch the full message
        # This would require additional calls to the Microsoft Graph API
        # For simplicity, we're just returning None for other headers
        return None
    
    def get_email_content(self, message: Dict[str, Any]) -> Optional[str]:
        """
        Extract the content from an Outlook message.
        
        Args:
            message: The message dictionary
            
        Returns:
            The email content, or None if not found
        """
        # Microsoft Graph API provides content directly
        if 'body' in message:
            content_type = message['body'].get('contentType', '').lower()
            content = message['body'].get('content', '')
            
            # If it's plain text but we need HTML, convert it
            if content and content_type == 'text':
                content = f"<html><body><pre>{content}</pre></body></html>"
            
            return content
        return None
    
    def get_attachments(self, message: Dict[str, Any], download_dir: str = './attachments') -> List[Dict[str, str]]:
        """
        Download attachments from an Outlook message.
        
        Args:
            message: The message dictionary
            download_dir: Directory to save attachments
            
        Returns:
            List of dictionaries with attachment information
        """
        try:
            client = self.authenticator.authenticate()
            message_id = message.get('id')
            
            if not message_id:
                return []
            
            # Create download directory if it doesn't exist
            if not os.path.exists(download_dir):
                os.makedirs(download_dir)
            
            attachments = []
            
            # Check if the message has attachments expanded
            if 'attachments' in message:
                for attachment in message['attachments']:
                    attachment_id = attachment.get('id')
                    name = attachment.get('name', 'unnamed')
                    content_type = attachment.get('contentType', 'application/octet-stream')
                    
                    # Create a safe filename
                    safe_filename = os.path.basename(''.join(c for c in name if c.isalnum() or c in '._- '))
                    
                    # Ensure unique filename
                    file_path = os.path.join(download_dir, safe_filename)
                    if os.path.exists(file_path):
                        base_name, ext = os.path.splitext(safe_filename)
                        safe_filename = f"{base_name}_{message_id}{ext}"
                        file_path = os.path.join(download_dir, safe_filename)
                    
                    # Get attachment content
                    if 'contentBytes' in attachment:
                        # Attachment content is already in the response
                        content_bytes = base64.b64decode(attachment['contentBytes'])
                    else:
                        # Need to fetch attachment content
                        attachment_request = client.get(f"/me/messages/{message_id}/attachments/{attachment_id}")
                        
                        if attachment_request.status_code == 200:
                            attachment_data = attachment_request.json()
                            content_bytes = base64.b64decode(attachment_data.get('contentBytes', ''))
                        else:
                            logger.error(f"Error fetching attachment: {attachment_request.status_code}")
                            continue
                    
                    # Save the attachment
                    with open(file_path, 'wb') as f:
                        f.write(content_bytes)
                    
                    attachments.append({
                        'filename': safe_filename,
                        'path': file_path,
                        'mime_type': content_type,
                        'size': len(content_bytes)
                    })
            
            return attachments
            
        except Exception as e:
            logger.error(f"Error downloading attachments: {e}")
            return []


# Data classes for parser results
@dataclass
class InvoiceDetails:
    """Base class for invoice details."""
    pass


@dataclass
class UtilityInvoiceDetails(InvoiceDetails):
    """Details for utility company invoices."""
    amount: Optional[float] = None
    iban: Optional[str] = None
    due_date: Optional[str] = None
    communication_code: Optional[str] = None


@dataclass
class ServiceInvoiceDetails(InvoiceDetails):
    """Details for service provider invoices."""
    date_paid: Optional[str] = None
    receipt_number: Optional[str] = None
    period: Optional[str] = None
    total: Optional[float] = None
    currency: str = '€'


@dataclass
class InvoiceResult:
    """Result of invoice parsing operation."""
    status: str
    count: int
    message: Optional[str] = None
    invoices: List[Dict[str, Any]] = field(default_factory=list)


# Abstract base class for invoice parsers
class InvoiceParser(abc.ABC):
    """Abstract base class for invoice parsers."""
    
    def __init__(self, email_fetcher: EmailFetcher):
        """
        Initialize the invoice parser.
        
        Args:
            email_fetcher: EmailFetcher instance
        """
        self.email_fetcher = email_fetcher
        self.debug = False
    
    @property
    @abc.abstractmethod
    def DEFAULT_QUERY(self) -> str:
        """Default query for fetching relevant invoices."""
        pass
    
    @abc.abstractmethod
    def extract_invoice_details(self, html_content: str, message_id: str = None) -> InvoiceDetails:
        """
        Extract invoice details from email HTML content.
        
        Args:
            html_content: The HTML email content
            message_id: Optional message ID for debugging
            
        Returns:
            InvoiceDetails instance with extracted data
        """
        pass
    
    def get_invoices(self, 
                     max_results: int = 100, 
                     debug: bool = False, 
                     download_attachments: bool = False, 
                     attachment_dir: str = './invoices') -> InvoiceResult:
        """
        Get invoices matching the parser's criteria.
        
        Args:
            max_results: Maximum number of invoices to fetch
            debug: Enable debug output
            download_attachments: Whether to download attachments
            attachment_dir: Directory to save attachments
            
        Returns:
            InvoiceResult with status and invoice details
        """
        self.debug = debug
        
        try:
            # Fetch emails
            if self.debug:
                logger.info(f"Fetching emails with query: {self.DEFAULT_QUERY}")
            
            messages = self.email_fetcher.fetch_emails(self.DEFAULT_QUERY, max_results)
            
            if not messages:
                if self.debug:
                    logger.info("No messages found matching the query")
                return InvoiceResult(
                    status='success',
                    message='No invoices found',
                    count=0,
                    invoices=[]
                )
            
            if self.debug:
                logger.info(f"Found {len(messages)} messages matching the query")
            
            # Create attachment directory if needed
            if download_attachments and not os.path.exists(attachment_dir):
                os.makedirs(attachment_dir)
            
            # Process each message
            invoices = []
            for message in messages:
                msg_id = message.get('id')
                subject = self.email_fetcher.get_header_value(message, 'Subject') or 'No Subject'
                
                if self.debug:
                    logger.info(f"\nProcessing message: {msg_id} - {subject}")
                
                # Get email content
                content = self.email_fetcher.get_email_content(message)
                
                # Extract details
                details = self.extract_invoice_details(content, msg_id) if content else None
                
                # Download attachments if requested
                attachments = []
                if download_attachments:
                    attachments = self.email_fetcher.get_attachments(message, attachment_dir)
                
                # Add to invoices list
                invoice_entry = {
                    'id': msg_id,
                    'subject': subject,
                    'details': details.__dict__ if details else {}
                }
                
                if download_attachments:
                    invoice_entry['attachments'] = attachments
                
                invoices.append(invoice_entry)
            
            return InvoiceResult(
                status='success',
                count=len(invoices),
                invoices=invoices
            )
            
        except Exception as e:
            error_msg = str(e)
            if self.debug:
                logger.error(f"Error in get_invoices: {error_msg}")
                import traceback
                traceback.print_exc()
            
            return InvoiceResult(
                status='error',
                message=error_msg,
                count=0,
                invoices=[]
            )


# Concrete parser implementations
class AnthropicBillParser(InvoiceParser):
    """Specialized parser for Anthropic invoice emails."""
    
    DEFAULT_QUERY = "from:invoice+statements@mail.anthropic.com subject:Your receipt from Anthropic"
    
    def extract_invoice_details(self, html_content: str, message_id: str = None) -> ServiceInvoiceDetails:
        """
        Extract invoice details from Anthropic email HTML content.
        
        Args:
            html_content: The HTML email content
            message_id: Optional message ID for debugging
            
        Returns:
            ServiceInvoiceDetails with extracted data
        """
        # Initialize empty details
        details = ServiceInvoiceDetails()
        
        if not html_content:
            if self.debug:
                logger.info(f"Message {message_id}: No HTML content found")
            return details
        
        try:
            # Create BeautifulSoup object
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract receipt number
            receipt_spans = soup.find_all('span', string=lambda text: text and text.strip().startswith('Receipt #'))
            if receipt_spans:
                receipt_text = receipt_spans[0].get_text(strip=True)
                receipt_match = re.search(r'Receipt #(\S+)', receipt_text)
                if receipt_match:
                    details.receipt_number = receipt_match.group(1)
        
            # Extract date paid
            date_paid_spans = soup.find_all('span', string=lambda text: text and 'Paid ' in text if text else False)
            if date_paid_spans:
                date_text = date_paid_spans[0].get_text(strip=True)
                date_match = re.search(r'Paid (.+)', date_text)
                if date_match:
                    details.date_paid = date_match.group(1)
            
            # Extract period
            period_spans = soup.find_all('span', string=lambda text: text and ' – ' in text and re.search(r'\w+ \d+\s*–\s*\w+ \d+, \d{4}', text) if text else False)
            if period_spans:
                details.period = period_spans[0].get_text(strip=True).replace('\u2013', '-')

            # Extract total amount
            total_spans = soup.find_all('span', string=lambda text: text and text.strip().isdigit() or (text.strip().startswith('€') and re.search(r'[€$£¥]\s*[\d,.]+', text.strip())) if text else False)
            for span in total_spans:
                amount_text = span.get_text(strip=True)
                currency_match = re.search(r'([€$£¥])\s*([\d,.]+)', amount_text)
                if currency_match:
                    details.currency = currency_match.group(1)
                    amount_str = currency_match.group(2).replace(',', '.')
                    try:
                        details.total = float(amount_str)
                        break
                    except ValueError:
                        continue
            
            if self.debug:
                logger.info(f"Extracted details: {details}")
            
            return details
            
        except Exception as e:
            if self.debug:
                logger.error(f"Error extracting invoice details: {e}")
                import traceback
                traceback.print_exc()
            return details


class ProximusInvoiceParser(InvoiceParser):
    """Specialized parser for Proximus invoice emails."""
    
    DEFAULT_QUERY = "from:billing.service@proximus.com subject:Votre décompte"
    
    def extract_invoice_details(self, html_content: str, message_id: str = None) -> UtilityInvoiceDetails:
        """
        Extract invoice details from Proximus email HTML content.
        
        Args:
            html_content: The HTML email content
            message_id: Optional message ID for debugging
            
        Returns:
            UtilityInvoiceDetails with extracted data
        """
        # Initialize empty details
        details = UtilityInvoiceDetails()
        
        if not html_content:
            if self.debug:
                logger.info(f"Message {message_id}: No HTML content found")
            return details
        
        try:
            # Create BeautifulSoup object
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Method 1: Handle side-by-side tables (special case for Proximus emails)
            # Find all table rows with spans
            labels_table = None
            values_table = None
            
            # Look for the pattern of two tables side by side in the same row
            for tr in soup.find_all('tr'):
                tds = tr.find_all('td', recursive=False)
                if len(tds) >= 2:
                    # Check if each TD contains a table
                    left_tables = tds[0].find_all('table', recursive=False)
                    right_tables = tds[1].find_all('table', recursive=False) if len(tds) > 1 else []
                    
                    if left_tables and right_tables:
                        # These might be our label and value tables
                        labels_table = left_tables[0]
                        values_table = right_tables[0]
                        
                        # Verify this is likely the invoice data by checking content
                        labels_text = labels_table.get_text().lower()
                        if ('montant' in labels_text or 'total' in labels_text) and ('compte' in labels_text or 'iban' in labels_text):
                            if self.debug:
                                logger.info("Found potential side-by-side tables with invoice data")
                            break
            
            if labels_table and values_table:
                # Extract labels and values from corresponding rows
                label_rows = labels_table.find_all('tr')
                value_rows = values_table.find_all('tr')
                
                # Make sure we have the same number of rows in both tables
                min_rows = min(len(label_rows), len(value_rows))
                
                # Map labels to field names
                label_mapping = {
                    'montant total': 'amount',
                    'numéro de compte': 'iban',
                    'communication': 'communication_code',
                    'echéance': 'due_date',
                    'échéance': 'due_date'
                }
                
                # Process each row pair
                for i in range(min_rows):
                    label_text = label_rows[i].get_text(strip=True).lower()
                    value_text = value_rows[i].get_text(strip=True)
                    
                    # Skip empty values
                    if not value_text:
                        continue
                    
                    # Determine which field this corresponds to
                    matched_field = None
                    for label_key, field_name in label_mapping.items():
                        if label_key in label_text:
                            matched_field = field_name
                            break
                    
                    if matched_field:
                        if matched_field == 'amount':
                            # Extract and convert amount
                            amount_match = re.search(r'(\d+[,.]\d+)', value_text)
                            if amount_match:
                                details.amount = float(amount_match.group(1).replace(',', '.'))
                                if self.debug:
                                    logger.info(f"Side-by-side tables: Found amount = {details.amount}")
                        
                        elif matched_field == 'iban':
                            # Extract IBAN
                            iban_match = re.search(r'(BE\d{2}[\s\d]{10,18})', value_text)
                            if iban_match:
                                details.iban = iban_match.group(1).replace('\r\n', '')
                                if self.debug:
                                    logger.info(f"Side-by-side tables: Found iban = {details.iban}")
                        
                        elif matched_field == 'communication_code':
                            # Extract communication code
                            comm_match = re.search(r'(\+{3}\d{3}/\d{4}/\d{5}\+{3})', value_text)
                            if comm_match:
                                details.communication_code = comm_match.group(1)
                                if self.debug:
                                    logger.info(f"Side-by-side tables: Found communication_code = {details.communication_code}")
                            # Try without +++ symbols
                            elif not details.communication_code:
                                comm_match = re.search(r'(\d{3}/\d{4}/\d{5})', value_text)
                                if comm_match:
                                    details.communication_code = f"+++{comm_match.group(1)}+++"
                                    if self.debug:
                                        logger.info(f"Side-by-side tables: Found communication_code = {details.communication_code}")
                        
                        elif matched_field == 'due_date':
                            # Extract due date
                            date_match = re.search(r'(\d{2}/\d{2}/\d{4})', value_text)
                            if date_match:
                                details.due_date = date_match.group(1)
                                if self.debug:
                                    logger.info(f"Side-by-side tables: Found due_date = {details.due_date}")
            
            # Return the extracted details
            if self.debug:
                logger.info(f"Final extracted details: {details}")
            
            return details
            
        except Exception as e:
            if self.debug:
                logger.error(f"Error extracting invoice details: {e}")
                import traceback
                traceback.print_exc()
            return details


class TotalEnergiesInvoiceParser(InvoiceParser):
    """Specialized parser for TotalEnergies invoice emails."""
    
    DEFAULT_QUERY = "from:invoice@post.totalenergies.be subject:Facture"
    
    def extract_invoice_details(self, html_content: str, message_id: str = None) -> UtilityInvoiceDetails:
        """
        Extract invoice details from TotalEnergies email HTML content.
        
        Args:
            html_content: The HTML email content
            message_id: Optional message ID for debugging
            
        Returns:
            UtilityInvoiceDetails with extracted data
        """
        # Initialize empty details
        details = UtilityInvoiceDetails()
        
        if not html_content:
            if self.debug:
                logger.info(f"Message {message_id}: No HTML content found")
            return details
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Helper function to extract with pattern and context
            def extract_with_pattern(pattern, context_terms=None):
                # Direct pattern search in all spans
                for span in soup.find_all('span'):
                    text = span.get_text(strip=True)
                    match = re.search(pattern, text)
                    if match:
                        return match.group(1)
                
                # If context terms provided, search with context
                if context_terms:
                    for span in soup.find_all('span'):
                        text = span.get_text(strip=True).lower()
                        if any(term.lower() in text for term in context_terms):
                            # Check this span and next span for the pattern
                            for check_span in [span, span.find_next('span')]:
                                if check_span:
                                    match = re.search(pattern, check_span.get_text(strip=True))
                                    if match:
                                        return match.group(1)
                
                return None
            
            # Extract IBAN, due date, and communication code
            details.iban = extract_with_pattern(
                r'(BE\d{2}\s*\d{4}\s*\d{4}\s*\d{4})', 
                ['notre compte', 'iban']
            )
            
            details.due_date = extract_with_pattern(
                r'(\d{2}/\d{2}/\d{4})',
                ['échéance', 'echeance', 'date limite']
            )
            
            details.communication_code = extract_with_pattern(
                r'(\d{3}/\d{4}/\d{5})',
                ['structurée', 'structuree', 'communication']
            )
            
            # Extract amount using multiple strategies
            amount_pattern = r'(\d+,\d+)'
            
            for span in soup.find_all('span'):
                text = span.get_text(strip=True)
                if re.search(amount_pattern, text) and '€' in text:
                    amount_match = re.search(r'TOTAL(.*)€', text)
                    if amount_match:
                        details.amount = float(amount_match.group(1).replace(',', '.'))
                        break
            
            return details
            
        except Exception as e:
            if self.debug:
                logger.error(f"Error extracting invoice details: {e}")
                import traceback
                traceback.print_exc()
            return details


# Factory for email providers
class EmailProviderFactory:
    """Factory for creating email providers."""
    
    @staticmethod
    def create_gmail_provider(client_secrets_file: str, token_file: str = None) -> Tuple[EmailAuthenticator, EmailFetcher]:
        """
        Create Gmail provider components.
        
        Args:
            client_secrets_file: Path to client secrets file
            token_file: Optional path to token file
            
        Returns:
            Tuple of (EmailAuthenticator, EmailFetcher)
        """
        token = token_file or f"gmail_token_{os.path.basename(client_secrets_file)}.json"
        authenticator = GmailAuthenticator(client_secrets_file, token)
        fetcher = GmailFetcher(authenticator)
        return authenticator, fetcher
    
    @staticmethod
    def create_outlook_provider(client_id: str, client_secret: str, tenant_id: str, token_file: str = None) -> Tuple[EmailAuthenticator, EmailFetcher]:
        """
        Create Outlook provider components.
        
        Args:
            client_id: Azure AD app client ID
            client_secret: Azure AD app client secret
            tenant_id: Azure AD tenant ID
            token_file: Optional path to token file
            
        Returns:
            Tuple of (EmailAuthenticator, EmailFetcher)
        """
        token = token_file or f"outlook_token_{client_id}.json"
        authenticator = OutlookAuthenticator(client_id, client_secret, tenant_id, token)
        fetcher = OutlookFetcher(authenticator)
        return authenticator, fetcher


# Parser registry
class ParserRegistry:
    """Registry for available invoice parsers."""
    
    PARSERS = {
        'anthropic': AnthropicBillParser,
        'proximus': ProximusInvoiceParser,
        'totalenergies': TotalEnergiesInvoiceParser
    }
    
    @classmethod
    def get_parser(cls, name: str, email_fetcher: EmailFetcher) -> Optional[InvoiceParser]:
        """
        Get a parser by name.
        
        Args:
            name: Parser name
            email_fetcher: EmailFetcher instance to use
            
        Returns:
            InvoiceParser instance or None if not found
        """
        parser_class = cls.PARSERS.get(name.lower())
        if parser_class:
            return parser_class(email_fetcher)
        return None
    
    @classmethod
    def get_all_parsers(cls, email_fetcher: EmailFetcher) -> Dict[str, InvoiceParser]:
        """
        Get all available parsers.
        
        Args:
            email_fetcher: EmailFetcher instance to use
            
        Returns:
            Dictionary of parser name -> InvoiceParser instance
        """
        return {name: parser_class(email_fetcher) for name, parser_class in cls.PARSERS.items()}


# Example usage
def main():
    """Example usage of the invoice parsers with both Gmail and Outlook."""
    try:
        
                
        gmail_auth_pro, gmail_fetcher_pro = EmailProviderFactory.create_gmail_provider(
            client_secrets_file='client_secret_427481450688-hp9r8nikvlu9du9jkl6g59ihvpv55i6b.apps.googleusercontent.com.json',
            token_file='token_irvinheslanpro.json'
        )
        parser = ParserRegistry.get_parser('totalenergies', gmail_fetcher_pro)
        result = parser.get_invoices(debug=True, download_attachments=False)
        print(json.dumps(result.__dict__, indent=2))
        
        
        
        # Set up Gmail provider
        gmail_auth, gmail_fetcher = EmailProviderFactory.create_gmail_provider(
            client_secrets_file='client_secret_427481450688-hp9r8nikvlu9du9jkl6g59ihvpv55i6b.apps.googleusercontent.com.json',
            token_file='token_irvhes.json'
        )
        
        # Create parser for Anthropic invoices
        # parser = ParserRegistry.get_parser('anthropic', gmail_fetcher)
        # if not parser:
        #     print("Parser not found!")
        #     return
        
        # # Get invoices
        # result = parser.get_invoices(debug=True, download_attachments=False)
        
        # # Print results
        # print(json.dumps(result.__dict__, indent=2))
        

        parser = ParserRegistry.get_parser('proximus', gmail_fetcher)
        result = parser.get_invoices(debug=True, download_attachments=False)
        print(json.dumps(result.__dict__, indent=2))

        
        return result
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return InvoiceResult(
            status='error',
            message=str(e),
            count=0,
            invoices=[]
        )


if __name__ == "__main__":
    main()